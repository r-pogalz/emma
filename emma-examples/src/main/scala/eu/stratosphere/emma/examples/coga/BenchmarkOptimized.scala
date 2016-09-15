import java.util.concurrent.{CountDownLatch, TimeUnit}

object BenchmarkOptimized {

  val usage =
    """
    Usage: [--warm-up num] [--rounds num] [--num-threads num] [--debug true] filename
    """

  val defaultWarmup = 10
  val defaultRounds = 20
  val defaultNumThreads = 1

  val warmupSym = 'warmup
  val roundsSym = 'rounds
  val numThreadsSym = 'numthreads
  val debugSym = 'debug
  val infileSym = 'infile

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
          toOptionMap(map ++ Map(warmupSym -> value.toInt),
                      tail)
        case "--rounds" :: value :: tail =>
          toOptionMap(map ++ Map(roundsSym -> value.toInt),
                      tail)
        case "--num-threads" :: value :: tail =>
          toOptionMap(map ++ Map(numThreadsSym -> value.toInt),
                      tail)
        case "--debug" :: value :: tail =>
          toOptionMap(map ++ Map(debugSym -> value.toBoolean),
                      tail)
        case string :: opt2 :: tail if isSwitch(opt2) =>
          toOptionMap(map ++ Map(infileSym -> string),
                      list.tail)
        case string :: Nil => toOptionMap(map ++ Map('infile -> string),
                                          list.tail)
        case option :: tail => throw new IllegalArgumentException("Unknown option " + option)
      }
    }
    val options = toOptionMap(Map(),
                              arglist)

    val bufferedSource = scala.io.Source.fromFile(options(infileSym).asInstanceOf[String])
    val buffer = collection.mutable.ArrayBuffer.newBuilder[Lineitem]
    var cnt = 0
    for (line <- bufferedSource.getLines) {
      cnt += 1
      if (cnt % 100000 == 0) println(s"Read $cnt lines")
      val cols = line.split("\\|").map(_.trim)
      buffer += Lineitem(cols(0).toInt,
                         cols(1).toInt,
                         cols(2).toInt,
                         cols(3).toInt,
                         cols(4).toInt,
                         cols(5).toDouble,
                         cols(6).toDouble,
                         cols(7).toDouble,
                         cols(8),
                         cols(9),
                         cols(10),
                         cols(11),
                         cols(12),
                         cols(13),
                         cols(14),
                         cols(15))
    }
    bufferedSource.close
    val lineitems = buffer.result().toArray

    compute(lineitems,
            options.getOrElse(warmupSym,
                              defaultWarmup).asInstanceOf[Int],
            options.getOrElse(roundsSym,
                              defaultRounds).asInstanceOf[Int],
            options.getOrElse(numThreadsSym,
                              defaultNumThreads).asInstanceOf[Int],
            options.getOrElse(debugSym,
                              false).asInstanceOf[Boolean])
  }

  private def compute(lineitems: Array[Lineitem],
                      warmupRounds: Int,
                      times: Int,
                      numThreads: Int,
                      debug: Boolean) = {
    val durationsBuffer = Seq.newBuilder[Long]

    //preparation for threads
    val results = new Array[Double](numThreads)
    val threads = new Array[Thread](numThreads)
    val minItemsPerThread = lineitems.length / numThreads
    val maxItemsPerThread = minItemsPerThread + 1;
    val threadsWithMaxItems = lineitems.length % numThreads

    for (currRound <- 1 to (warmupRounds + times)) {
      //create threads (split work evenly)
      var start = 0
      val countDownLatch = new CountDownLatch(numThreads)
      for (i <- 0 until numThreads) {
        val itemsCount = if (i < threadsWithMaxItems) maxItemsPerThread else minItemsPerThread
        val end = start + itemsCount
        threads(i) = new Thread(new ComputationThread(lineitems,
                                                      i,
                                                      results,
                                                      start,
                                                      end,
                                                      countDownLatch))
        start = end
      }

      //start to profile
      val t0 = System.nanoTime()
      var i = 0
      while (i < numThreads) {
        threads(i).start()
        i += 1
      }
      //wait until threads finished execution
      countDownLatch.await()
      val avgDisc = results.sum / lineitems.length
      val t1 = System.nanoTime()

      val duration = TimeUnit.NANOSECONDS.toMillis(t1 - t0)
      
      if (debug) println(s"Average Disc is $avgDisc")
      
      //discard warm up rounds from measurement
      if (currRound <= warmupRounds) {
        println(s"Execution time(Warm up round $currRound): ${duration}ms")
      } else {
        durationsBuffer += duration
        //      computed.foreach(println)
        println(s"Execution time(Round ${currRound - warmupRounds}): ${duration}ms")
      }
      //clear result array
      for (k <- 0 until results.length) results(k) = 0.0
    }
    val durations = durationsBuffer.result()

    println("===========================SUMMARY==============================")
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

  class ComputationThread(lineitems: Array[Lineitem],
                          i: Int,
                          results: Array[Double],
                          start: Int,
                          end: Int,
                          countDownLatch: CountDownLatch) extends Runnable {

    def run = {
      //computation
      var j = start
      var sum = 0.0
      while (j < end) {
        sum += lineitems(j).extendedPrice * (1 - lineitems(j).discount)
        j += 1
      }
      results(i) = sum
      countDownLatch.countDown()
    }

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
