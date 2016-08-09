package eu.stratosphere.emma.examples.coga

import eu.stratosphere.emma.api._
import eu.stratosphere.emma.examples.Algorithm
import eu.stratosphere.emma.examples.text.WordCount
import eu.stratosphere.emma.runtime.Engine
import net.sourceforge.argparse4j.inf.{Namespace, Subparser}

import scala.math.{pow, sqrt}


/**
  *
  * Implements the "EuclideanDist" program that computes a simple word occurrence histogram
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
class EuclideanDist(input: String, output: String, rt: Engine) extends Algorithm(rt) {

  def this(ns: Namespace, rt: Engine) = this(
    ns.get[String](WordCount.Command.KEY_INPUT),
    ns.get[String](WordCount.Command.KEY_OUTPUT),
    rt)

  def run() = {

    val alg = emma.parallelize {
      val points = read(s"$input/points.csv", new CSVInputFormat[Point]('|'))

      val distances = for {
        left <- points
        right <- points
      } yield (sqrt(pow(left.x - right.x, 2) + pow(left.y - right.y, 2)))


//            val distances = for {
//              left <- points
//              right <- points
//            } yield {
//              val cast = left.x / 2
//              val cast2 = left.x + 2
//              val cast3 = left.x - 2
//              val cast4 = left.x * 2
//              val short = pow(left.x - right.x, 2)
//              val pow2 = pow(left.y - right.y, 2)
//              sqrt(short + pow2)
//            }


      //      val distances = for {
      //        left <- points
      //      } yield (Point(left.x*2, left.y*2))
      //      val dist2 = for {
      //        left <- distances
      //      } yield (Point(left.y, left.x))


      //            val distances = for {
      //              left <- points
      //            } yield {
      //              val d = 12.0d
      //              val c = 'C'
      //              val l = 12l
      //              val fl = 12.2f
      //              val hello = "hello"
      //              val world = "world"
      //              val helloWorld = hello + world
      //              val num = 8
      //              val pl = num + 10
      //              pow(2, 2)
      //            }

      //            val distances = for {
      //              left <- points
      //            } yield {
      //              var d = 4
      //              if (d == 1) {
      //                d += 2
      //              } else if(d == 2) {
      //                d -= 1
      //              }
      //              (d ^ 2, d*5, sqrt(d))
      //              Point(left.x, 2)
      //            }


      println(distances)
      // write the results into a CSV file
      distances.writeCsv(s"$output/distances.csv")
    }

    alg.run(rt)
  }
}

case class Point(x: Double, y: Double)

object EuclideanDist {

  class Command extends Algorithm.Command[EuclideanDist] {

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
