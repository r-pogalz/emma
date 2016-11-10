import java.io.{File, FileInputStream}
import java.net.URI
import java.util.concurrent.TimeUnit

import eu.stratosphere.emma.api.{CSVInputFormat, _}

import scala.language.implicitConversions
import scala.math.pow

object MicroBenchmark {

  val usage =
    """
    Usage: [--warm-up num] [--rounds num] [--sf num] [--debug true] path
    """

  val defaultWarmup = 5
  val defaultRounds = 10
  val defaultSf = 1

  var debug = false

  val warmupSym = 'warmup
  val roundsSym = 'rounds
  val sfSym = 'sf
  val debugSym = 'debug
  val pathSym = 'path

  var lineitemsSize = 0

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
        case "--sf" :: value :: tail =>
          toOptionMap(map ++ Map(sfSym -> value.toInt), tail)
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
    val scaleFactor: Int = options.getOrElse(sfSym, defaultSf).asInstanceOf[Int]
    val tblPath: String = options
                          .getOrElse(pathSym, throw new IllegalArgumentException("No path to tbl files specified"))
                          .asInstanceOf[String]

    var lineitems = read(s"$tblPath/lineitem.tbl", new CSVInputFormat[Lineitem]('|'))
    lineitemsSize = lineitems.size
    println("read lineitem tbl successfully")

    profile(warmupRnds, measureRnds, filterLineitem(lineitems, (l: Lineitem) => l.partKey <= scaleFactor * 2000),
            "Filter 0.01")

    profile(warmupRnds, measureRnds, filterLineitem(lineitems, (l: Lineitem) => l.partKey <= scaleFactor * 20000),
            "Filter 0.1")

    profile(warmupRnds, measureRnds, filterLineitem(lineitems, (l: Lineitem) => l.partKey <= scaleFactor * 100000),
            "Filter 0.5")

    profile(warmupRnds, measureRnds, filterLineitem(lineitems, (l: Lineitem) => l.partKey <= scaleFactor * 200000),
            "Filter 1.0")

    profile(warmupRnds, measureRnds, mapLineitem(lineitems, simpleComputation), "Simple Map")

    profile(warmupRnds, measureRnds, mapLineitem(lineitems, murmurHash3), "MurmurHash3 Map")
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
      //call gc
      System.gc()
      System.runFinalization()
      
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

    }
    
    //remove min and max value from time values
    var durations = durationsBuffer.result()
    durations = durations diff List(durations.min)
    durations = durations diff List(durations.max)

    println(s"===========================SUMMARY==============================")
    val n = durations.size
    val avg = durations.sum / n
    val variance = durations.map(d => pow(d - avg,
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
  

  def murmurHash3(l: Lineitem) = {
    val rot32 = (x: Int, y: Int) => (x * pow(2, y)).toInt | (x / pow(2, 32 - y)).toInt
    val len = 4
    val c1 = 0xcc9e2d51
    val c2 = 0x1b873593
    val r1 = 15
    val r2 = 13
    val m = 5

    val n = 0xe6546b64

    var hash = 0xdeadbeef //initialize with seed
    val values = Seq(l.orderKey, l.partKey, l.suppKey, l.lineNumber)

    var k = 0

    values.foreach { v =>
      k = v
      k *= c1
      k = rot32(k, r1)
      k *= c2
      
      hash ^= k
      hash = rot32(hash, r2) * m + n
                   }
    
    hash ^= len
    hash ^= (hash / pow(2, 16)).toInt
    hash *= 0x85ebca6b
    hash ^= (hash / pow(2, 13)).toInt
    hash *= 0xc2b2ae35
    hash ^= (hash / pow(2, 16)).toInt
    
    hash
  }

  def filterLineitem(lineitems: Seq[Lineitem], predicate: (Lineitem) => Boolean) = {
    if (debug) println("executing lineitem selection")

    val result = lineitems.filter(predicate)

    if (debug) println(s"Selection Result Size: ${result.size}")

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
                      returnFlag: Char, //1
                      lineStatus: Char, //1
                      shipDate: String, //10
                      commitDate: String, //10
                      receiptDate: String, //10
                      shipInstruct: String, //25
                      shipMode: String, //10
                      comment: String) //var 44

}
