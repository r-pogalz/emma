package eu.stratosphere.emma.examples.coga

import java.util.concurrent.TimeUnit

import scala.collection.parallel._
import scala.collection.parallel.mutable.ParArray

object Benchmark {

  val usage =
    """
    Usage: [--warm-up num] [--rounds num] [--num-threads num] filename
    """

  val defaultWarmup = 10
  val defaultRounds = 20
  val defaultNumThreads = 1

  val warmupSym = 'warmup
  val roundsSym = 'rounds
  val numThreadsSym = 'numthreads
  val infileSym = 'infile

  val confidenceCriticalValue = 2.325

  def main(args: Array[String]) = {
    if (args.length == 0) println(usage)
    val arglist = args.toList
    type OptionMap = Map[Symbol, Any]

    def toOptionMap(map: OptionMap, list: List[String]): OptionMap = {
      def isSwitch(s: String) = (s(0) == '-')
      list match {
        case Nil => map
        case "--warm-up" :: value :: tail =>
          toOptionMap(map ++ Map(warmupSym -> value.toInt), tail)
        case "--rounds" :: value :: tail =>
          toOptionMap(map ++ Map(roundsSym -> value.toInt), tail)
        case "--num-threads" :: value :: tail =>
          toOptionMap(map ++ Map(numThreadsSym -> value.toInt), tail)
        case string :: opt2 :: tail if isSwitch(opt2) =>
          toOptionMap(map ++ Map('infile -> string), list.tail)
        case string :: Nil => toOptionMap(map ++ Map(infileSym -> string), list.tail)
        case option :: tail => throw new IllegalArgumentException("Unknown option " + option)
      }
    }
    val options = toOptionMap(Map(), arglist)

    val bufferedSource = scala.io.Source.fromFile(options(infileSym).asInstanceOf[String])
    val buffer = mutable.ParArray.newBuilder[Part]
    for (line <- bufferedSource.getLines) {
      val cols = line.split("\\|").map(_.trim)
      buffer += Part(cols(0), cols(1), cols(2), cols(3), cols(4), cols(5).toInt, cols(6), cols(7).toDouble, cols(8))
    }
    bufferedSource.close
    val parts = buffer.result()
    parts.tasksupport = new ForkJoinTaskSupport(
      new scala.concurrent.forkjoin.ForkJoinPool(options.getOrElse(numThreadsSym, defaultNumThreads).asInstanceOf[Int]))

    compute(parts, options.getOrElse(warmupSym, defaultWarmup).asInstanceOf[Int],
      options.getOrElse(roundsSym, defaultRounds).asInstanceOf[Int])
  }

  private def compute(parts: ParArray[Part], warmupRounds: Int, times: Int) = {
    val durationsBuffer = Seq.newBuilder[Long]
    for (i <- 1 to (warmupRounds + times)) {
      //actual algorithm to profile
      val t0 = System.nanoTime()
      val computed = parts.map(part => part.pSize * part.pRetailPrice)
      val t1 = System.nanoTime()

      val duration = TimeUnit.NANOSECONDS.toMillis(t1 - t0)
      //discard warm up rounds from measurement
      if (i <= warmupRounds) {
        println(s"Execution time(Warm up round $i): $duration ms")
      } else {
        durationsBuffer += duration
        //      computed.foreach(println)
        println(s"Execution time(Round ${i - warmupRounds}): $duration ms")
      }
    }
    val durations = durationsBuffer.result()

    println("===========================SUMMARY==============================")
    val n = durations.size
    val avg = durations.sum / n
    val variance = durations.map(d => scala.math.pow(d - avg, 2.0)).sum / n
    val deviation = scala.math.sqrt(variance)
    //calculate 99%-confidence-interval
    val confidenceBorder = confidenceCriticalValue * (deviation / scala.math.sqrt(n))
    val lowerBound = avg - confidenceBorder
    val upperBound = avg + confidenceBorder

    println(s"Min execution time: ${durations.min} ms")
    println(s"Max execution time: ${durations.max} ms")
    println(s"Average execution time: $avg ms")
    println(s"Standard deviation: $deviation ms")
    println(s"99% confidence interval: [$lowerBound, $upperBound]")
  }

  case class Part(pPartKey: String, pName: String, pMfgr: String, pBrand: String, pType: String, pSize: Int,
                  pContainer: String, pRetailPrice: Double, pComment: String)

}
