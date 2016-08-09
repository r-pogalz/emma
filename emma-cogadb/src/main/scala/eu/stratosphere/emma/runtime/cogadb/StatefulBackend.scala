package eu.stratosphere.emma.runtime.cogadb

import eu.stratosphere.emma.api.DataBag
import eu.stratosphere.emma.api.model.Identity
import eu.stratosphere.emma.runtime.AbstractStatefulBackend
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.java.io.{TypeSerializerInputFormat, TypeSerializerOutputFormat}
import org.apache.flink.api.scala._
import org.apache.flink.core.fs.FileSystem
import org.apache.flink.util.Collector

import scala.reflect.ClassTag

class StatefulBackend[S <: Identity[K] : ClassTag : TypeInformation, K: ClassTag : TypeInformation]
        (env: ExecutionEnvironment, xs: DataSet[S]) extends AbstractStatefulBackend[S, K] {

  private val typeInformation = xs.getType()


  // Create initial file
  writeStateToFile(xs)


  // todo: figure out how to delete the files (we can't delete it here in the update functions,
  // because with haven't executed the dataflow yet)
  // We also can't just overwrite the old file, because the write might be occuring in parallel with the read
  // (at least as far as the Flink runtime is concerned, I mean the read
  // probably keeps the file open while the topology is up)

  def updateWithZero[O: ClassTag : TypeInformation]
    (udf: S => DataBag[O]): DataSet[O] = {

    val javaUdfResults = readStateFromFile().flatMap(
      (state, out: Collector[Either[S, O]]) => {
        for (o <- udf(state).fetch())
          out.collect(Right(o))
        out.collect(Left(state))
      })

    writeNewState(javaUdfResults)
    filterRight(javaUdfResults)
  }

  def updateWithOne[U: ClassTag : TypeInformation, O: ClassTag : TypeInformation]
    (updates: DataSet[U], updateKeySelector: U => K, udf: (S, U) => DataBag[O]): DataSet[O] = {

    val javaUdfResults = readStateFromFile().coGroup(updates).where(s => s.identity).equalTo(updateKeySelector).apply(
      (stateIt: Iterator[S], updIt: Iterator[U], out: Collector[Either[S, O]]) => {
        if (stateIt.hasNext) {
          val state = stateIt.next()
          for (u <- updIt) {
            for (o <- udf(state, u).fetch())
              out.collect(Right(o))
          }
          out.collect(Left(state))
        }
      })

    writeNewState(javaUdfResults)
    filterRight(javaUdfResults)
  }

  def updateWithMany[U: ClassTag : TypeInformation, O: ClassTag : TypeInformation]
        (updates: DataSet[U], updateKeySelector: U => K, udf: (S, DataBag[U]) => DataBag[O]): DataSet[O] = {

    val javaUdfResults = readStateFromFile().coGroup(updates).where(s => s.identity).equalTo(updateKeySelector).apply(
      (stateIt: Iterator[S], updIt: Iterator[U], out: Collector[Either[S, O]]) => {
        if (stateIt.hasNext) {
          val state = stateIt.next()
          val upds = DataBag(updIt.toSeq)
          for (o <- udf(state, upds).fetch())
            out.collect(Right(o))
          out.collect(Left(state))
        }
    })

    writeNewState(javaUdfResults)
    filterRight(javaUdfResults)
  }

  def fetchToStateLess() : DataSet[S] = {
    readStateFromFile()
  }



  private def readStateFromFile() : DataSet[S] = {
    env.readFile(new TypeSerializerInputFormat[S](typeInformation), currentFileName())
  }

  private def writeNewState[O: ClassTag : TypeInformation](javaUdfResults: DataSet[Either[S, O]]) = {
    val newState = javaUdfResults.flatMap ((x, out: Collector[S]) => x match {
      case Left(l) => out.collect(l)
      case Right(_) =>
    })

    seqNumber += 1
    writeStateToFile(newState)
  }

  private def writeStateToFile(state : DataSet[S]) = {
    state.write(new TypeSerializerOutputFormat[S](), currentFileName(), FileSystem.WriteMode.NO_OVERWRITE)
  }

  private def filterRight[O: ClassTag : TypeInformation](s: DataSet[Either[S, O]]): DataSet[O] = {
    s.flatMap ((x, out: Collector[O]) => x match {
      case Left(_) =>
      case Right(r) => out.collect(r)
    })
  }
}
