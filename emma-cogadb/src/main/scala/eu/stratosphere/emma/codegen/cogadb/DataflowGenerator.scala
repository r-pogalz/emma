package eu.stratosphere.emma.codegen.cogadb

import java.io.{File, _}
import java.net.Socket
import java.nio.file.Files.copy
import java.nio.file.Paths.get
import java.nio.file.StandardCopyOption.REPLACE_EXISTING
import java.util.UUID

import eu.stratosphere.emma.api.{CSVInputFormat, CSVOutputFormat, ParallelizedDataBag, TextInputFormat}
import eu.stratosphere.emma.codegen.cogadb.UDFParser.UDFClosure
import eu.stratosphere.emma.codegen.utils.DataflowCompiler
import eu.stratosphere.emma.ir
import eu.stratosphere.emma.macros.RuntimeUtil
import eu.stratosphere.emma.runtime.logger
import org.apache.flink.api.scala.{DataSet, ExecutionEnvironment}

import scala.collection.mutable
import scala.io.Codec
import scala.reflect.runtime.universe._
import scala.sys.process._

class DataflowGenerator(val compiler: DataflowCompiler, val sessionID: UUID = UUID.randomUUID)
  extends RuntimeUtil {

  import syntax._

  val tb = compiler.tb
  private val memo = mutable.Map.empty[String, ModuleSymbol]

  private val env = $"env$$flink"
  private val ENV = typeOf[ExecutionEnvironment]

  private val resMgr = $"resMgr"
  private val RES_MGR = typeOf[TempResultsManager]

  private val compile = (tree: Tree) =>
    compiler.compile(tree.as[ImplDef]).asModule

  private var path = ""

  // Type constructors
  val DATA_SET = typeOf[DataSet[Nothing]].typeConstructor

  private val sharedCoGaFolder: String = "/media/sf_CoGaShared/euclideandist"

  // --------------------------------------------------------------------------
  // Combinator Dataflows (traversal based)
  // --------------------------------------------------------------------------

  def generateDataflowDef(root: ir.Combinator[_], id: String) = {
    val dataFlowName = id
    memo.getOrElseUpdate(dataFlowName, {
      logger.info("Generating dataflow code for '{}'", dataFlowName)
      // initialize UDF store to be passed around implicitly during dataFlow
      // opCode assembly
      implicit val closure = new DataFlowClosure()
      // generate dataFlow operator assembly code
      val opCode = generateOpCode(root)
      val f = $"f$$flink"
      val F = mk.freshType("F$flink")

      //CoGaDB run script
      //      val load1 = Source.fromFile(s"$path/load_script_1.json").getLines.mkString
      //      val load2 = Source.fromFile(s"$path/load_script_2.json").getLines.mkString
      //      val map = Source.fromFile(s"$path/map_script.json").getLines.mkString
      //      val export = Source.fromFile(s"$path/export_script.json").getLines.mkString
      //      val output = "ls -la".!!

      // create a sorted list of closure parameters
      // (convention used by client in the generated macro)
      val params = for {
        (name, tpt) <- closure.closureParams.toSeq sortBy {
          _._1.toString
        }
      } yield q"val $name: $tpt"

      // create a sorted list of closure local inputs
      // (convention used by the Engine client)
      val localInputs = for {
        (name, tpt) <- closure.localInputParams.toSeq sortBy {
          _._1.toString
        }
      } yield q"val $name: $tpt"

      val runMethod = root match {
        case op: ir.Fold[_, _] =>
          q"""def run($env: $ENV, $resMgr: $RES_MGR, ..$params, ..$localInputs) = {
            $opCode
          }"""

        case op: ir.TempSink[_] =>
          val result = $"result$$flink"
          q"""def run($env: $ENV, $resMgr: $RES_MGR, ..$params, ..$localInputs) = {
            val $result = $opCode
            $env.execute(${s"Emma[$sessionID][$dataFlowName]"})
            $env.createInput[${typeOf(op.tag).dealias}]($result)
          }"""

        case op: ir.Write[_] =>
          q"""def run($env: $ENV, $resMgr: $RES_MGR, ..$params, ..$localInputs) = {
            $opCode
            $env.execute(${s"Emma[$sessionID][$dataFlowName]"})
            ()
          }"""

        // todo: this won't work if there are multiple statefuls in one comprehension
        case op: ir.StatefulCreate[_, _] =>
          val result = $"result$$flink"
          q"""def run($env: $ENV, $resMgr: $RES_MGR, ..$params, ..$localInputs) = {
            val $result = $opCode
            $env.execute(${s"Emma[$sessionID][$dataFlowName]"})
            $result
          }"""

        case _ => throw new RuntimeException(
          s"Unsupported root IR node type '${root.getClass}'")
      }

      // assemble dataFlow
      q"""object ${TermName(dataFlowName)} {
        import _root_.org.apache.flink.api.scala._

        $runMethod

        def clean[$F]($env: $ENV, $f: $F) = {
          if ($env.getConfig.isClosureCleanerEnabled)
            _root_.org.apache.flink.api.java.ClosureCleaner.clean($f, true)

          _root_.org.apache.flink.api.java.ClosureCleaner.ensureSerializable($f)
          $f
        }
      }""" ->> compile
    })
  }

  private def generateOpCode(combinator: ir.Combinator[_])
                            (implicit closure: DataFlowClosure): Tree = combinator match {
    case op: ir.Read[_] => opCode(op)
    case op: ir.Write[_] => opCode(op)
    case op: ir.TempSource[_] => opCode(op)
    case op: ir.TempSink[_] => opCode(op)
    case op: ir.Scatter[_] => opCode(op)
    case op: ir.Map[_, _] => opCode(op)
    case op: ir.FlatMap[_, _] => opCode(op)
    case op: ir.Filter[_] => opCode(op)
    case op: ir.EquiJoin[_, _, _] => opCode(op)
    case op: ir.Cross[_, _, _] => opCode(op)
    case op: ir.Fold[_, _] => opCode(op)
    case op: ir.FoldGroup[_, _] => opCode(op)
    case op: ir.Distinct[_] => opCode(op)
    case op: ir.Union[_] => opCode(op)
    case op: ir.Group[_, _] => opCode(op)
    case op: ir.StatefulCreate[_, _] => opCode(op)
    case op: ir.StatefulFetch[_, _] => opCode(op)
    case op: ir.UpdateWithZero[_, _, _] => opCode(op)
    case op: ir.UpdateWithOne[_, _, _, _] => opCode(op)
    case op: ir.UpdateWithMany[_, _, _, _] => opCode(op)
    case _ => throw new RuntimeException(
      s"Unsupported ir node of type '${combinator.getClass}'")
  }

  private def opCode[B](op: ir.Read[B])
                       (implicit closure: DataFlowClosure): Tree = {
    // infer types and generate type information
    val T = typeOf(op.tag).precise
    val inFormatTree = op.format match {
      case fmt: TextInputFormat[_] =>
        val inFmt = $"inFormat$$flink"

        q"""{
          val $inFmt =
            new _root_.org.apache.flink.api.java.io.TextInputFormat(
              new _root_.org.apache.flink.core.fs.Path(${op.location}))

          $inFmt.setDelimiter(${fmt.separator})
          $inFmt
        }"""

      case fmt: CSVInputFormat[_] =>
        if (!(T <:< weakTypeOf[Product]))
          throw new RuntimeException(
            s"Cannot create CoGaDB CsvInputFormat for non-product type ${typeOf(op.tag)}")

        //for COGADB
        path = new File(op.location).getParent
        val loadScript = scala.io.Source.fromFile(s"$path/read_template.json").getLines.mkString

        val inputFileName = new File(op.location).getName
        copy(get(op.location), get(s"/Users/rpogalz/CoGaShared/euclideandist/$inputFileName"), REPLACE_EXISTING)
        val substitute1 = loadScript.format("CROSS" +
          "$1_1", "CROSS$1_1", inputFileName)
        val substitute2 = loadScript.format("CROSS$1_2", "CROSS$1_2", inputFileName)

        scala.tools.nsc.io.File("/Users/rpogalz/CoGaShared/euclideandist/load_script_1.json")(codec = Codec.ISO8859).
          writeAll(substitute1)
        scala.tools.nsc.io.File("/Users/rpogalz/CoGaShared/euclideandist/load_script_2.json")(codec = Codec.ISO8859).
          writeAll(substitute2)

        executeOnCoGaDB(s"$sharedCoGaFolder/load_script_1.json")
        executeOnCoGaDB(s"$sharedCoGaFolder/load_script_2.json")
        //        sendMsg("execute_query_from_json /media/sf_CoGaShared/euclideandist/load_script_1.json")
        //        sendMsg("execute_query_from_json /media/sf_CoGaShared/euclideandist/load_script_2.json")

        //FLINK
        val inFmt = $"inFmt$$flink"
        q"""{
          val $inFmt =
            new _root_.org.apache.flink.api.scala.operators.ScalaCsvInputFormat[$T](
              new _root_.org.apache.flink.core.fs.Path(${op.location}),
              _root_.org.apache.flink.api.scala.createTypeInformation[$T])

          $inFmt.setFieldDelimiter(${fmt.separator})
          $inFmt
        }"""

      case _ => throw new RuntimeException(
        s"Unsupported InputFormat of type '${op.format.getClass}'")
    }

    // assemble dataFlow fragment
    q"$env.createInput($inFormatTree)"
  }

  private def executeOnCoGaDB(jsonPath: String) = {
    val output = (s"echo execute_query_from_json $jsonPath" #|
      "nc localhost 8000").!!
    println(output)
  }

  private def sendMsg(msg: String) = {
    val s = new Socket("192.168.161.40", 8000)
    val in = new BufferedReader(new InputStreamReader(s.getInputStream()))
    val out = new PrintStream(s.getOutputStream())

    out.println(msg)
    out.flush()
    println("Received:")
    var input = ""
    while ((input = in.readLine()) != null) {
      println(input)
    }

    s.close()
  }

  private def opCode[A](op: ir.Write[A])
                       (implicit closure: DataFlowClosure): Tree = {
    val T = typeOf(op.xs.tag).precise
    // assemble input fragment
    val xs = generateOpCode(op.xs)

    val outFormatTree = op.format match {
      case fmt: CSVOutputFormat[_] =>
        //CoGaDB
        val exportTemplate = scala.io.Source.fromFile(s"$path/export_template.json").getLines.mkString
        //        val substitute = exportTemplate.format(op.location, "DIST")
        val substitute = exportTemplate.format("DIST")

        scala.tools.nsc.io.File("/Users/rpogalz/CoGaShared/euclideandist/export_script.json")(codec = Codec.ISO8859).
          writeAll(substitute)
        //        executeOnCoGaDB(s"$sharedCoGaFolder/export_script.json")
        //        sendMsg("execute_query_from_json /media/sf_CoGaShared/euclideandist/export_script.json")

        //Flink
        if (!(T <:< weakTypeOf[Product]))
          throw new RuntimeException(
            s"Cannot create Flink CsvOutputFormat for non-product type $T")

        q"""new _root_.org.apache.flink.api.scala.operators.ScalaCsvOutputFormat[$T](
          new _root_.org.apache.flink.core.fs.Path(${op.location}),
          ${fmt.separator}.toString)"""

      case _ => throw new RuntimeException(
        s"Unsupported InputFormat of type '${op.format.getClass}'")
    }

    // assemble dataFlow fragment
    q"""$xs.write($outFormatTree, ${op.location},
      _root_.org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE)"""
  }

  private def opCode[B](op: ir.TempSource[B])
                       (implicit closure: DataFlowClosure): Tree = {
    // infer types and generate type information
    val T = typeOf(op.tag).precise
    // add a dedicated closure variable to pass the input param
    val param = TermName(op.ref.as[ParallelizedDataBag[B, DataSet[B]]].name)
    val inFmt = $"inFmt$$flink"
    closure.closureParams +=
      param -> tq"_root_.eu.stratosphere.emma.api.ParallelizedDataBag[$T, ${DATA_SET(T)}]"

    // assemble dataFlow fragment
    q"""{
      val $inFmt = new _root_.org.apache.flink.api.java.io.TypeSerializerInputFormat[$T](
        _root_.org.apache.flink.api.scala.createTypeInformation[$T])

      $inFmt.setFilePath($resMgr.resolve($param.name))
      $env.createInput($inFmt)
    }"""
  }

  private def opCode[A](op: ir.TempSink[A])
                       (implicit closure: DataFlowClosure): Tree = {
    // infer types and generate type information
    val T = typeOf(op.tag).precise
    // assemble input fragment
    val xs = generateOpCode(op.xs)
    val $(input, ti, inFmt, outFmt) =
      $("input$flink", "typeInfo$flink", "inFmt$flink", "outFmt$flink")
    // assemble dataFlow fragment
    q"""{
      val $input = $xs
      val $ti = _root_.org.apache.flink.api.scala.createTypeInformation[$T]
      val $outFmt = new _root_.org.apache.flink.api.java.io.TypeSerializerOutputFormat[$T]
      $outFmt.setInputType($ti, $env.getConfig)
      $outFmt.setSerializer($ti.createSerializer($env.getConfig))

      $input.write($outFmt, $resMgr.assign(${op.name}),
        _root_.org.apache.flink.core.fs.FileSystem.WriteMode.OVERWRITE)

      val $inFmt = new _root_.org.apache.flink.api.java.io.TypeSerializerInputFormat[$T](
        _root_.org.apache.flink.api.scala.createTypeInformation[$T])

      $inFmt.setFilePath($resMgr.resolve(${op.name}))
      $inFmt
    }"""
  }

  private def opCode[B](op: ir.Scatter[B])
                       (implicit closure: DataFlowClosure): Tree = {
    // infer types and generate type information
    val T = typeOf(op.tag).precise
    // add a dedicated closure variable to pass the scattered term
    val input = $"scatter$$flink"
    closure.localInputParams += input -> tq"_root_.scala.Seq[$T]"
    // assemble dataFlow fragment
    q"$env.fromCollection[$T]($input)"
  }

  private def opCode[B, A](op: ir.Map[B, A])
                          (implicit closure: DataFlowClosure): Tree = {
    // assemble input fragment
    val xs = generateOpCode(op.xs)
    // generate fn UDF
    val mapFun = parseCheck(op.f)
    //    val fun = comp.tb.parse(op.f)
    //    val anf = Core.anf(fun)
    //    val checked = comp.Type.check(fun)
    //    val anf = typeCheckAndANF(checked)

    //extract input params of UDF
    val input = UDFParser.extractInputParams(mapFun)
    implicit val udfClosure = new UDFClosure()
    udfClosure.inputMapping += "cross$1._1.x" -> "cross$1_1.x"
    udfClosure.inputMapping += "cross$1._1.y" -> "cross$1_1.y"
    udfClosure.inputMapping += "cross$1._2.x" -> "cross$1_2.x"
    udfClosure.inputMapping += "cross$1._2.y" -> "cross$1_2.y"

    //CoGaDB UDF compilation
    val transformedMapFun = UDFParser.modifiedTree(mapFun)
    val cogaMapUDF = CoGaCodeGenerator.generateCode(transformedMapFun._1)
    println(cogaMapUDF)
    //read map template and substitute mapUDF
    val mapTemplate = scala.io.Source.fromFile(s"$path/map_template.json").getLines.mkString
    val cogaMapOperator = mapTemplate.format(transformedMapFun._2(0),
      transformedMapFun._2(0), transformedMapFun._2(0), cogaMapUDF, "CROSS$1_1", "CROSS$1_2")
    scala.tools.nsc.io.File("/Users/rpogalz/CoGaShared/euclideandist/map_script.json")(codec = Codec.ISO8859).
      writeAll(cogaMapOperator)

    executeOnCoGaDB(s"$sharedCoGaFolder/map_script.json")
    //    sendMsg("execute_query_from_json /media/sf_CoGaShared/euclideandist/map_script.json")

    //Flink
    val mapUDF = ir.UDF(mapFun, mapFun.preciseType, tb)
    closure.capture(mapUDF)
    q"$xs.map(${mapUDF.func})"
  }


  private def opCode[B, A](op: ir.FlatMap[B, A])
                          (implicit closure: DataFlowClosure): Tree = {
    // assemble input fragment
    val xs = generateOpCode(op.xs)
    // generate fn UDF
    val fmFun = parseCheck(op.f)
    val fmUDF = ir.UDF(fmFun, fmFun.preciseType, tb)
    closure.capture(fmUDF)
    // assemble dataFlow fragment
    q"$xs.flatMap((..${fmUDF.params}) => ${fmUDF.body}.fetch())"
  }

  private def opCode[A](op: ir.Filter[A])
                       (implicit closure: DataFlowClosure): Tree = {
    // assemble input fragment
    val xs = generateOpCode(op.xs)
    // generate fn UDF
    val predicate = parseCheck(op.p)
    val fUDF = ir.UDF(predicate, predicate.preciseType, tb)
    closure.capture(fUDF)
    // assemble dataFlow fragment
    q"$xs.filter(${fUDF.func})"
  }

  private def opCode[C, A, B](op: ir.EquiJoin[C, A, B])
                             (implicit closure: DataFlowClosure): Tree = {
    // assemble input fragments
    val xs = generateOpCode(op.xs)
    val ys = generateOpCode(op.ys)
    val kx = parseCheck(op.keyx).as[Function]
    val ky = parseCheck(op.keyy).as[Function]
    // assemble dataFlow fragment
    q"$xs.join($ys).where(${kx.body}).equalTo(${ky.body})"
  }

  private def opCode[C, A, B](op: ir.Cross[C, A, B])
                             (implicit closure: DataFlowClosure): Tree = {
    // assemble input fragments
    val xs = generateOpCode(op.xs)
    val ys = generateOpCode(op.ys)
    // assemble dataFlow fragment
    q"$xs.cross($ys)"
  }

  private def opCode[B, A](op: ir.Fold[B, A])
                          (implicit closure: DataFlowClosure): Tree = {
    // assemble input fragment
    val xs = generateOpCode(op.xs)
    // get fold components
    val empty = parseCheck(op.empty)
    val sng = parseCheck(op.sng)
    val union = parseCheck(op.union)
    // create UDFs
    val emptyUDF = ir.UDF(empty, empty.preciseType, tb)
    val mapUDF = ir.UDF(sng, sng.preciseType, tb)
    val foldUDF = ir.UDF(union, union.preciseType, tb)
    // capture closures
    closure.capture(emptyUDF, mapUDF, foldUDF)
    val result = $"res$$flink"
    // assemble dataFlow fragment
    q"""{
      val $result = $xs.map(${mapUDF.func}).reduce(${foldUDF.func}).collect()
      if ($result.isEmpty) ${empty.as[Function].body} else $result.head
    }"""
  }

  private def opCode[B, A](op: ir.FoldGroup[B, A])
                          (implicit closure: DataFlowClosure): Tree = {
    val SRC = typeOf(op.xs.tag).precise
    val DST = typeOf(op.tag).precise
    // assemble input fragment
    val xs = generateOpCode(op.xs)
    // get fold components
    val key = parseCheck(op.key)
    val keyUDF = ir.UDF(key, key.preciseType, tb)
    val empty = parseCheck(op.empty)
    val emptyUDF = ir.UDF(empty, empty.preciseType, tb)
    val sng = parseCheck(op.sng)
    val sngUDF = ir.UDF(sng, sng.preciseType, tb)
    val union = parseCheck(op.union)
    val unionUDF = ir.UDF(union, union.preciseType, tb)
    val $(x, y) = $("x$flink", "y$flink")
    val mapFun = q"($x: $SRC) => new $DST(${keyUDF.func}($x), ${sngUDF.func}($x))"
    val foldUDF = {
      // assemble UDF code
      val udf =
        q"""() => ($x: $DST, $y: $DST) => {
        val ${unionUDF.params(0).name} = $x.values
        val ${unionUDF.params(1).name} = $y.values
        new $DST($x.key, ${unionUDF.body})
      }""".reTypeChecked
      // construct UDF
      ir.UDF(udf.as[Function], udf.preciseType, tb)
    }

    // add closure parameters
    closure.capture(keyUDF, emptyUDF, sngUDF, foldUDF)
    // assemble dataFlow fragment
    q"$xs.map($mapFun).groupBy(_.key).reduce(${foldUDF.func})"
  }

  private def opCode[A](op: ir.Distinct[A])
                       (implicit closure: DataFlowClosure): Tree = {
    // assemble input fragment
    val xs = generateOpCode(op.xs)
    // assemble dataFlow fragment
    q"$xs.distinct(x => x)"
  }

  private def opCode[A](op: ir.Union[A])
                       (implicit closure: DataFlowClosure): Tree = {
    // assemble input fragments
    val xs = generateOpCode(op.xs)
    val ys = generateOpCode(op.ys)
    // assemble dataFlow fragment
    q"$xs.union($ys)"
  }

  private def opCode[B, A](op: ir.Group[B, A])
                          (implicit closure: DataFlowClosure): Tree = {
    val SRC = typeOf(op.xs.tag).precise
    // assemble input fragment
    val xs = generateOpCode(op.xs)
    val $(iterator, buffer) = $("iter$flink", "buffer$flink")
    // generate key UDF
    val keyFun = parseCheck(op.key)
    val keyUDF = ir.UDF(keyFun, keyFun.preciseType, tb)
    closure.capture(keyUDF)
    // assemble dataFlow fragment
    q"""$xs.groupBy(${keyUDF.func}).reduceGroup({
      ($iterator: _root_.scala.Iterator[$SRC]) =>
        val $buffer = $iterator.toBuffer
        _root_.eu.stratosphere.emma.api.Group(
          ${keyUDF.func}($buffer.head),
          _root_.eu.stratosphere.emma.api.DataBag($buffer))
    })"""
  }

  private def opCode[A, K](op: ir.StatefulCreate[A, K])
                          (implicit closure: DataFlowClosure): Tree = {
    val S = typeOf(op.tagS).precise
    val K = typeOf(op.tagK).precise
    // assemble input fragment
    val xs = generateOpCode(op.xs)
    q"new _root_.eu.stratosphere.emma.runtime.flink.StatefulBackend[$S, $K]($env, $xs)"
  }

  private def opCode[S, K](op: ir.StatefulFetch[S, K])
                          (implicit closure: DataFlowClosure): Tree = {
    val S = typeOf(op.tag).precise
    val K = typeOf(op.tagK).precise
    closure.closureParams +=
      TermName(op.name) -> TypeTree(typeOf(op.tagAbstractStatefulBackend).precise)

    q"""${TermName(op.name)}
      .asInstanceOf[_root_.eu.stratosphere.emma.runtime.flink.StatefulBackend[$S, $K]]
      .fetchToStateLess()"""
  }

  private def opCode[S, K, U, O](op: ir.UpdateWithZero[S, K, O])
                                (implicit closure: DataFlowClosure): Tree = {
    closure.closureParams +=
      TermName(op.name) -> TypeTree(typeOf(op.tagAbstractStatefulBackend).precise)

    val updFun = parseCheck(op.udf)
    val updUDF = ir.UDF(updFun, updFun.preciseType, tb)
    val S = typeOf(op.tagS).precise
    val K = typeOf(op.tagK).precise
    val R = typeOf(op.tag).precise
    closure.capture(updUDF)
    q"""${TermName(op.name)}
      .asInstanceOf[_root_.eu.stratosphere.emma.runtime.flink.StatefulBackend[$S, $K]]
      .updateWithZero[$R](${updUDF.func})"""
  }

  private def opCode[S, K, U, O](op: ir.UpdateWithOne[S, K, U, O])
                                (implicit closure: DataFlowClosure): Tree = {
    closure.closureParams +=
      TermName(op.name) -> TypeTree(typeOf(op.tagAbstractStatefulBackend).precise)

    val updates = generateOpCode(op.updates)
    val keyFun = parseCheck(op.updateKeySel)
    val keyUdf = ir.UDF(keyFun, keyFun.preciseType, tb)
    val updFun = parseCheck(op.udf)
    val updUDF = ir.UDF(updFun, updFun.preciseType, tb)
    val S = typeOf(op.tagS).precise
    val K = typeOf(op.tagK).precise
    val U = typeOf(op.tagU).precise
    val R = typeOf(op.tag).precise
    closure.capture(keyUdf)
    closure.capture(updUDF)
    q"""${TermName(op.name)}
      .asInstanceOf[_root_.eu.stratosphere.emma.runtime.flink.StatefulBackend[$S, $K]]
      .updateWithOne[$U, $R]($updates, ${keyUdf.func}, ${updUDF.func})"""
  }

  private def opCode[S, K, U, O](op: ir.UpdateWithMany[S, K, U, O])
                                (implicit closure: DataFlowClosure): Tree = {
    closure.closureParams +=
      TermName(op.name) -> TypeTree(typeOf(op.tagAbstractStatefulBackend).precise)

    val updates = generateOpCode(op.updates)
    val keyFun = parseCheck(op.updateKeySel)
    val keyUDF = ir.UDF(keyFun, keyFun.preciseType, tb)
    val updFun = parseCheck(op.udf)
    val updUDF = ir.UDF(updFun, updFun.preciseType, tb)
    val S = typeOf(op.tagS).precise
    val K = typeOf(op.tagK).precise
    val U = typeOf(op.tagU).precise
    val R = typeOf(op.tag).precise
    closure.capture(keyUDF)
    closure.capture(updUDF)
    q"""${TermName(op.name)}
      .asInstanceOf[_root_.eu.stratosphere.emma.runtime.flink.StatefulBackend[$S, $K]]
      .updateWithMany[$U, $R]($updates, ${keyUDF.func}, ${updUDF.func})"""
  }

  // --------------------------------------------------------------------------
  // Auxiliary structures
  // --------------------------------------------------------------------------

  private class DataFlowClosure {
    val closureParams = mutable.Map.empty[TermName, Tree]
    val localInputParams = mutable.Map.empty[TermName, Tree]

    def capture(fs: ir.UDF*) = for (f <- fs)
      closureParams ++= f.closure map { p => p.name -> p.tpt }
  }

}
