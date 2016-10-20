package eu.stratosphere.emma.examples.coga

import java.io.{File, FileInputStream}
import java.net.URI
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.concurrent.TimeUnit

import eu.stratosphere.emma.api.{CSVInputFormat, _}
import org.apache.commons.lang.time.DateUtils

import scala.collection.mutable.ListBuffer

object MicroBenchmark {

  val usage =
    """
    Usage: [--warm-up num] [--rounds num] [--num-threads num] [--debug true] path
    """

  val defaultWarmup = 10
  val defaultRounds = 20
  val defaultNumThreads = 1

  var debug = false

  val warmupSym = 'warmup
  val roundsSym = 'rounds
  val numThreadsSym = 'numthreads
  val debugSym = 'debug
  val pathSym = 'path

  val confidenceCriticalValue = 2.325

  def main(args: Array[String]) = {
    if (args.length == 0) println(usage)
    val arglist = args.toList
    type OptionMap = Map[Symbol, Any]

    def toOptionMap(map: OptionMap,
                    list: List[String]): OptionMap = {
      def isSwitch(s: String) = (s(0) == '-')
      list match {
        case Nil => map
        case "--warm-up" :: value :: tail =>
          toOptionMap(map ++ Map(warmupSym -> value.toInt), tail)
        case "--rounds" :: value :: tail =>
          toOptionMap(map ++ Map(roundsSym -> value.toInt), tail)
        case "--num-threads" :: value :: tail =>
          toOptionMap(map ++ Map(numThreadsSym -> value.toInt), tail)
        case "--debug" :: value :: tail =>
          toOptionMap(map ++ Map(debugSym -> value.toBoolean), tail)
        case string :: opt2 :: tail if isSwitch(opt2) =>
          toOptionMap(map ++ Map(pathSym -> string), list.tail)
        case string :: Nil => toOptionMap(map ++ Map(pathSym -> string), list.tail)
        case option :: tail => throw new IllegalArgumentException("Unknown option " + option)
      }
    }
    val options = toOptionMap(Map(), arglist)

    debug = options.getOrElse(debugSym, false).asInstanceOf[Boolean]

    val warmupRnds: Int = options.getOrElse(warmupSym, defaultWarmup).asInstanceOf[Int]
    val measureRnds: Int = options.getOrElse(roundsSym, defaultRounds).asInstanceOf[Int]
    val tblPath: String = options
                          .getOrElse(pathSym, throw new IllegalArgumentException("No path to tbl files specified"))
                          .asInstanceOf[String]

    var lineitems = read(s"$tblPath/lineitem.tbl", new CSVInputFormat[Lineitem]('|'))
    println("read lineitem tbl successfully")

    profile(warmupRnds, measureRnds, filterLineitem(lineitems, (l: Lineitem) => l.partKey <= 2000), "Filter 0.01")

    profile(warmupRnds, measureRnds, filterLineitem(lineitems, (l: Lineitem) => l.partKey <= 20000), "Filter 0.1")

    profile(warmupRnds, measureRnds, filterLineitem(lineitems, (l: Lineitem) => l.partKey <= 100000), "Filter 0.5")

    profile(warmupRnds, measureRnds, filterLineitem(lineitems, (l: Lineitem) => l.partKey <= 200000), "Filter 1.0")
    
    profile(warmupRnds, measureRnds, mapLineitem(lineitems, simpleComputation), "Simple Map")
    
    profile(warmupRnds, measureRnds, mapLineitem(lineitems, simpleComputation), "Complex Map")
  }

  def read[A](path: String, format: InputFormat[A]): Seq[A] = {
    val is = {
      val uri = new URI(path)
      new FileInputStream(new File(path))
    }

    format.read(is)
  }

  private def profile[A](warmupRounds: Int, times: Int, query: => Seq[A], title: String) = {

    println(s"===========================$title==============================")

    val durationsBuffer = Seq.newBuilder[Long]
    for (i <- 1 to (warmupRounds + times)) {
      //actual algorithm to profile
      val t0 = System.nanoTime()
      val res = query
      val t1 = System.nanoTime()

      val duration = TimeUnit.NANOSECONDS.toMillis(t1 - t0)
      //discard warm up rounds from measurement
      if (i <= warmupRounds) {
        println(s"Execution time(Warm up round $i): ${duration}ms")
      } else {
        durationsBuffer += duration
        //      computed.foreach(println)
        println(s"Execution time(Round ${i - warmupRounds}): ${duration}ms")
      }

      //call gc
      System.gc()
      System.runFinalization()
    }
    val durations = durationsBuffer.result()

    println(s"===========================SUMMARY==============================")
    val n = durations.size
    val avg = durations.sum / n
    val variance = durations.map(d => scala.math.pow(d - avg,
                                                     2.0)).sum / n
    val deviation = scala.math.sqrt(variance)
    //calculate 99%-confidence-interval
    val confidenceBorder = confidenceCriticalValue * (deviation / scala.math.sqrt(n))
    val lowerBound = avg - confidenceBorder
    val upperBound = avg + confidenceBorder

    println(s"Min execution time: ${durations.min}ms")
    println(s"Max execution time: ${durations.max}ms")
    println(s"Average execution time: ${avg}ms")
    println(s"Standard deviation: ${deviation}ms")
    println(s"99% confidence interval (in ms): [$lowerBound, $upperBound]")
  }

  def mapLineitem[Result](lineitems: Seq[Lineitem], mapFun: (Lineitem) => Result) = {
    if (debug) println("executing lineitem map function")

    val result = for (l <- lineitems) yield mapFun

    if (debug) println(s"Map Results: $result")

    result
  }

  //computes disc price
  def simpleComputation(l: Lineitem) = l.extendedPrice * (1 - l.discount)

  //hash calculation by folding on a string (4 bytes at a time)
  def complexComputation(l: Lineitem, m: Int) = {
    val string = l.orderKey + l.partKey + l.suppKey + l.lineNumber + l.quantity + l.extendedPrice + l.discount +
                 l.tax + l.returnFlag + l.lineStatus + l.shipDate + l.commitDate + l.receiptDate + l.shipInstruct +
                 l.shipMode + l.comment

    val intLength = string.length / 4
    var sum = 0
    for (j <- 1 to intLength) {
      val c = string.substring(j * 4, (j * 4) + 4).toCharArray
      var mult = 1
      
      for (k <- 1 to c.length) {
        sum += c(k) * mult
        mult *= 256
      }
    }

    val c = string.substring(intLength * 4).toCharArray()
    var mult = 1
    for (k <- 1 to c.length) {
      sum += c(k) * mult
      mult *= 256
    }

    scala.math.abs(sum) % m
  }

  def filterLineitem(lineitems: Seq[Lineitem], predicate: (Lineitem) => Boolean) = {
    if (debug) println("executing lineitem selection")

    val result = lineitems.filter(predicate)

    if (debug) println(s"Selection Results: $result")

    result
  }


  case class Lineitem(orderKey: Int,
                      partKey: Int,
                      suppKey: Int,
                      lineNumber: Int,
                      quantity: Int,
                      extendedPrice: Double,
                      discount: Double,
                      tax: Double,
                      returnFlag: String,
                      lineStatus: String,
                      shipDate: String,
                      commitDate: String,
                      receiptDate: String,
                      shipInstruct: String,
                      shipMode: String,
                      comment: String)

}
