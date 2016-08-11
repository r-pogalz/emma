package eu.stratosphere.emma.examples.coga

import java.util.concurrent.{CountDownLatch, TimeUnit}

object BenchmarkOptimized {

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
          toOptionMap(map ++ Map(infileSym -> string), list.tail)
        case string :: Nil => toOptionMap(map ++ Map('infile -> string), list.tail)
        case option :: tail => throw new IllegalArgumentException("Unknown option " + option)
      }
    }
    val options = toOptionMap(Map(), arglist)

    val bufferedSource = scala.io.Source.fromFile(options(infileSym).asInstanceOf[String])
    val buffer = collection.mutable.ArrayBuffer.newBuilder[Part]
    for (line <- bufferedSource.getLines) {
      val cols = line.split("\\|").map(_.trim)
      buffer += Part(
        cols(0), cols(1), cols(2), cols(3), cols(4), cols(5).toInt, cols(6), cols(7).toDouble, cols(8))
    }
    bufferedSource.close
    val parts = buffer.result().toArray

    compute(parts, options.getOrElse(warmupSym, defaultWarmup).asInstanceOf[Int],
      options.getOrElse(roundsSym, defaultRounds).asInstanceOf[Int],
      options.getOrElse(numThreadsSym, defaultNumThreads).asInstanceOf[Int])
  }

  private def compute(parts: Array[Part],
                      warmupRounds: Int, times: Int, numThreads: Int) = {
    val durationsBuffer = Seq.newBuilder[Long]

    //preparation for threads
    val results = new Array[Double](parts.length)
    val threads = new Array[Thread](numThreads)
    val minItemsPerThread = parts.length / numThreads
    val maxItemsPerThread = minItemsPerThread + 1;
    val threadsWithMaxItems = parts.length % numThreads

    for (currRound <- 1 to (warmupRounds + times)) {
      //create threads (split work evenly)
      var start = 0
      val countDownLatch = new CountDownLatch(numThreads)
      for (i <- 0 until numThreads) {
        val itemsCount = if (i < threadsWithMaxItems) maxItemsPerThread else minItemsPerThread
        val end = start + itemsCount
        threads(i) = new Thread(new ComputationThread(parts, results, start, end, countDownLatch))
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
      val t1 = System.nanoTime()

      val duration = TimeUnit.NANOSECONDS.toMillis(t1 - t0)
      //discard warm up rounds from measurement
      if (currRound <= warmupRounds) {
        println(s"Execution time(Warm up round $currRound): $duration ms")
      } else {
        durationsBuffer += duration
        //      computed.foreach(println)
        println(s"Execution time(Round ${currRound - warmupRounds}): $duration ms")
      }
      //clear result array
      for (k <- 0 until results.length) results(k) = 0.0
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

  class ComputationThread(parts: Array[Part],
                          results: Array[Double], start: Int, end: Int,
                          countDownLatch: CountDownLatch) extends Runnable {

    def run = {
      //computation
      var j = start
      while (j < end) {
        results(j) = parts(j).pSize * parts(j).pRetailPrice
        j += 1
      }
      countDownLatch.countDown()
    }

  }

  case class Part(pPartKey: String, pName: String, pMfgr: String, pBrand: String, pType: String, pSize: Int,
                  pContainer: String, pRetailPrice: Double, pComment: String)

}
