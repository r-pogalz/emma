package eu.stratosphere.emma.examples.coga

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.examples.Algorithm
import eu.stratosphere.emma.examples.text.WordCount
import eu.stratosphere.emma.runtime.Engine
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}

import scala.math._


/**
  *
  * Implements a list of UDFs program that computes a simple word occurrence histogram
  * over text files.
  *
  * The input is a plain text file with lines separated by newline characters and the
  * output is a file containing words and the number of times that they occure in the
  * input text file.
  *
  * Usage:
  * {{{
  *   EuclideanDistance <text path> <result path>>
  * }}}
  *
  * This example shows how to:
  *
  * - write a very simple Emma program.
  *
  * @param output Output path
  */
class Concept(input: String, output: String, rt: Engine) extends Algorithm(rt) {

  def this(ns: Namespace, rt: Engine) = this(
    ns.get[String](WordCount.Command.KEY_INPUT),
    ns.get[String](WordCount.Command.KEY_OUTPUT),
    rt)

  def run() = {

    val alg = emma.parallelize {
      val part = read(s"$input/part.csv", new CSVInputFormat[Part]('|'))
      val supplier = read(s"$input/supplier.csv", new CSVInputFormat[Supplier]('|'))
      val partSupp = read(s"$input/partsupp.csv", new CSVInputFormat[PartSupp]('|'))

      //#1 test tuple output
      val tupleOutput = for {
        p <- part
        ps <- partSupp
        if p.pPartKey == ps.psPartKey
      } yield ((p.pName, ps.psAvailQty))

      println(tupleOutput)

      //#2 test complex output
      val complexOutput = for {
        s <- supplier
      } yield (Person(s.sName, s.sAddress, s.sPhone))

      println(complexOutput)

      //#3 test assignments
      val asgnmts = for {
        p <- part
      } yield {
        val text = p.pBrand
        val num = p.pSize
        val dec = p.pRetailPrice
        val isTrue = true
        val isFalse = false
        val long = 10l
        val float = 1.0f
        val char = 'a'
        (text, num, dec, isTrue, isFalse, long, float, char)
      }

      println(asgnmts)

      //#4 test arithmetic operation of integer and double (single input) -> result must be double
      val intMultDec = for {
        p <- part
      } yield {
        val res = p.pSize * p.pRetailPrice
        res
      }
      
      println(intMultDec)
      
      //#5 test precedence of multiplication and division (multiple input)
      val precedence = for {
          p <- part
        ps <- partSupp
        if p.pPartKey == ps.psPartKey
      } yield (ps.psSupplyCost / ps.psAvailQty + p.pSize * p.pRetailPrice)

      println(precedence)

      //#6 test string concatenation
      val concat = for {
        p <- part
        s <- supplier
        ps <- partSupp
        if p.pPartKey == ps.psPartKey
        if s.sSuppKey == ps.psSuppKey
      } yield {
        val lable = "Supplier, Part: "
        val partSuppName = lable + s.sName + p.pName
        partSuppName
      }

      println(concat)

      //#7 test string interpolation with string and number
      val strNumConcat = for {
        p <- part
      } yield
        (s"Name: ${p.pName}, Brand: ${p.pBrand}, Type: ${p.pType}, Size: ${p.pSize}, Retail Price: ${p.pRetailPrice}")

      println(strNumConcat)

      //#8 test unicode vs. ascii for variable names -> results in segmentation fault
      val unicodeVariableName = for {
        p <- part
      } yield {
        val α = p.pSize
        α
      }

      println(unicodeVariableName)

      //#9 test reserved keyword in C as variable name (e.g. auto, short, etc.) -> segmentation fault
      val reservedKeywords = for {
        p <- part
      } yield {
        val short = p.pSize
        short
      }

      println(reservedKeywords)
      
      //#10 test if else in statement
      val ifElse = for {
        s <- supplier
        ps <- partSupp
        if s.sSuppKey == ps.psSuppKey
      } yield {
        val res = if (ps.psSupplyCost > 100.00) s.sName else ps.psSupplyCost
        res
      }

      println(ifElse)

      //#11 test math library functions
      val mathLib = for {
        p <- part
        ps <- partSupp
        if p.pPartKey == ps.psPartKey
      } yield {
        (sqrt(ps.psSupplyCost),
          pow(p.pRetailPrice, 2),
          round(ps.psSupplyCost),
          floor(ps.psSupplyCost),
          ceil(ps.psSupplyCost),
          max(p.pRetailPrice, ps.psSupplyCost),
          min(p.pSize, ps.psAvailQty))
      }

      println(mathLib)

      //#12 test relational operators
      val strLib = for {
        p <- part
        s <- supplier
        ps <- partSupp
        if p.pPartKey == ps.psPartKey
        if s.sSuppKey == ps.psSuppKey
      } yield {
        (p.pSize == ps.psAvailQty,
          p.pSize != ps.psAvailQty,
          p.pSize < ps.psAvailQty,
          p.pSize > ps.psAvailQty,
          p.pSize <= ps.psAvailQty,
          p.pSize >= ps.psAvailQty,
          s.sSuppKey == ps.psSuppKey,
          s.sSuppKey != ps.psSuppKey,
          s.sName.substring(0, 1))
      }

      println(strLib)
      
      //#13 duplicate UDFs in same data flow with the same input types
      //same input types in the following UDFs
      //input comes from the original source part
      //in CoGaDB, output of this UDF will be written to the table <COMPUTED>
      val parts = for {
        p <- part
      } yield (Part("invalidKey", p.pName, p.pMfgr, p.pBrand, p.pType, -1, p.pContainer, 0.00, p.pComment))

      //input of this UDF actually are actually computed values and therefore read from the <COMPUTED> table in CoGaDB
      val parts2 = for {
        p <- parts
      } yield ((p.pPartKey, 1))

      println(parts2)

      // write the results into a CSV file
      //      distances.writeCsv(s"$output/distances.csv")
    }

    alg.run(rt)
  }
}

case class Part(pPartKey: String, pName: String, pMfgr: String, pBrand: String, pType: String, pSize: Int,
                pContainer: String, pRetailPrice: Double, pComment: String)

case class Supplier(sSuppKey: String, sName: String, sAddress: String, sNationKey: String, sPhone: String,
                    sAcctbal: Double, sComment: String)

case class PartSupp(psPartKey: String, psSuppKey: String, psAvailQty: Int, psSupplyCost: Double, psComment: String)

case class Person(name: String, address: String, phone: String)

object Concept {

  class Command extends Algorithm.Command[Concept] {

    // algorithm names
    override def name = "euclideandist"

    override def description = "Word Count Example"

    override def setup(parser: Subparser) = {
      // basic setup
      super.setup(parser)

      // add arguments
      parser.addArgument(Command.KEY_INPUT)
        .`type`[String](classOf[String])
        .dest(Command.KEY_INPUT)
        .metavar("INPATH")
        .help("base input file")
      parser.addArgument(Command.KEY_OUTPUT)
        .`type`[String](classOf[String])
        .dest(Command.KEY_OUTPUT)
        .metavar("OUTPUT")
        .help("output file")
    }
  }

  object Command {
    // argument names
    val KEY_INPUT = "input"
    val KEY_OUTPUT = "output"
  }

}
