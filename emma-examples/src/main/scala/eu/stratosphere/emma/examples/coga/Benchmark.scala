package eu.stratosphere.emma.examples.coga

import java.util.concurrent.TimeUnit

object Benchmark {

  def main(args: Array[String]) = {
    if (args.size != 3) {
      throw new IllegalArgumentException("Missing parameters.")
    }

    val bufferedSource = scala.io.Source.fromFile(args(0))
    val buffer = Seq.newBuilder[Part]
    for (line <- bufferedSource.getLines) {
      val cols = line.split("\\|").map(_.trim)
      buffer += Part(cols(0), cols(1), cols(2), cols(3), cols(4), cols(5).toInt, cols(6), cols(7).toDouble, cols(8))
    }
    bufferedSource.close
    val parts = buffer.result()

    compute(parts, args(1).toInt, args(2).toInt)
  }

  private def compute(parts: Seq[Part], warmupRounds: Int, times: Int) = {
    val durationsBuffer = Seq.newBuilder[Long]
    for (i <- 1 to (warmupRounds + times)) {
      //actual algorithm to profile
      val t0 = System.nanoTime()
      val computed = for (part <- parts) yield (part.pSize * part.pRetailPrice)
      val t1 = System.nanoTime()

      val duration = TimeUnit.NANOSECONDS.toMillis(t1 - t0)
      if (i <= warmupRounds) {
        println(s"Execution time(Warm up round $i): $duration ms")
      } else {
        durationsBuffer += duration
        //      computed.foreach(println)
        println(s"Execution time(Round $i): $duration ms")

      }
    }
    val durations = durationsBuffer.result()

    println("===========================SUMMARY==============================")
    val min = durations.min
    val max = durations.max
    val avg = durations.sum / times.toDouble

    println(s"Min execution time: $min ms")
    println(s"Max execution time: $max ms")
    println(s"Average execution time: $avg ms")
  }

  case class Part(pPartKey: String, pName: String, pMfgr: String, pBrand: String, pType: String, pSize: Int,
                  pContainer: String, pRetailPrice: Double, pComment: String)

}
