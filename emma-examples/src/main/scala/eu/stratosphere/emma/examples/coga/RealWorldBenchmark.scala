import java.io.{File, FileInputStream}
import java.net.URI
import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.concurrent.TimeUnit

import eu.stratosphere.emma.api.{CSVInputFormat, _}
import org.apache.commons.lang.time.DateUtils

import scala.collection.mutable.ListBuffer

object RealWorldBenchmark {

  val usage =
    """
    Usage: [--warm-up num] [--rounds num] [--debug true] path
    """

  val defaultWarmup = 5
  val defaultRounds = 10
  val defaultNumThreads = 1

  var debug = false

  val warmupSym = 'warmup
  val roundsSym = 'rounds
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

    //profile query01 from TPC-H
    profile(warmupRnds, measureRnds, query01(lineitems), "TPC-H Query01")

    //profile query06 from TPC-H
    profile(warmupRnds, measureRnds, query06(lineitems), "TPC-H Query06")

    var customers = read(s"$tblPath/customer.tbl", new CSVInputFormat[Customer]('|'))
    println("read customer tbl successfully")

    var orders = read(s"$tblPath/orders.tbl", new CSVInputFormat[Order]('|'))
    println("read orders tbl successfully")

    //profile query03 from TPC-H
    profile(warmupRnds, measureRnds, query03(customers, orders, lineitems), "TPC-H Query03")

    var suppliers = read(s"$tblPath/supplier.tbl", new CSVInputFormat[Supplier]('|'))
    println("read supplier tbl successfully")

    /*
    var parts = read(s"$tblPath/part.tbl", new CSVInputFormat[Part]('|'))
    println("read part tbl successfully")
    */

    var nations = read(s"$tblPath/nation.tbl", new CSVInputFormat[Nation]('|'))
    println("read nation tbl successfully")

    var regions = read(s"$tblPath/region.tbl", new CSVInputFormat[Region]('|'))
    println("read region tbl successfully")

    //profile query05 from TPC-H
    profile(warmupRnds,
            measureRnds,
            query05(customers,
                    orders,
                    lineitems,
                    suppliers,
                    nations,
                    regions),
            "TPC-H Query05")


    var points = read(s"$tblPath/points.csv", new CSVInputFormat[Point]('|'))
    var cntrds = read(s"$tblPath/centroids.csv", new CSVInputFormat[Point]('|'))

    //profile k-means
    profile(warmupRnds, measureRnds, kmeans(points, cntrds, 0.05), "K-Means Algorithm")
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

  /**
    * Original query:
    *
    * {{{
    * select
    *     l_returnflag,
    *     l_linestatus,
    *     sum(l_quantity) as sum_qty,
    *     sum(l_extendedprice) as sum_base_price,
    *     sum(l_extendedprice * (1 - l_discount)) as sum_disc_price,
    *     sum(l_extendedprice * (1 - l_discount) * (1 + l_tax)) as sum_charge,
    *     avg(l_quantity) as avg_qty,
    *     avg(l_extendedprice) as avg_price,
    *     avg(l_discount) as avg_disc,
    *     count(*) as count_order
    * from
    *     lineitem
    * where
    *     l_shipdate <= date '1998-12-01' - interval ':DELTA' day (3)
    * group by
    *     l_returnflag,
    *     l_linestatus
    * order by
    *     l_returnflag,
    *     l_linestatus;
    * }}}
    */
  def query01(lineitems: Seq[Lineitem]) = {

    if (debug) println("executing query01")

    val filteredLineitems = lineitems.filter(l => l.shipDate <= "1998-09-02")

    // aggregate and compute the final result
    val result = for {
      g <- group(filteredLineitems, (l: Lineitem) => new Query01Schema.GrpKey(l.returnFlag, l.lineStatus))
    } yield {
      // compute base aggregates
      val sumQty = g.values.map(_.quantity).sum
      val sumBasePrice = g.values.map(_.extendedPrice).sum
      val sumDiscPrice = g.values.map(l => l.extendedPrice * (1 - l.discount)).sum
      val sumCharge = g.values.map(l => l.extendedPrice * (1 - l.discount) * (1 + l.tax)).sum
      val countOrder = g.values.size
      // compute result
      Query01Schema.Result(g.key.returnFlag,
                           g.key.lineStatus,
                           sumQty,
                           sumBasePrice,
                           sumDiscPrice,
                           sumCharge,
                           avgQty = sumQty / countOrder,
                           avgPrice = sumBasePrice / countOrder,
                           avgDisc = sumDiscPrice / countOrder,
                           countOrder)
    }

    val sorted = result.sortBy(r => (r.returnFlag, r.lineStatus))

    if (debug) println(s"Query01 Results: $sorted")

    sorted
  }

  /**
    * Original query:
    *
    * {{{
    * select
    *     l_orderkey,
    *     sum(l_extendedprice * (1 - l_discount)) as revenue,
    *     o_orderdate,
    *     o_shippriority
    * from
    *     customer,
    *     orders,
    *     lineitem
    * where
    *     c_mktsegment = ':SEGMENT'
    *     and c_custkey = o_custkey
    *     and l_orderkey = o_orderkey
    *     and o_orderdate < date ':DATE'
    *     and l_shipdate > date ':DATE'
    * group by
    *     l_orderkey,
    *     o_orderdate,
    *     o_shippriority
    * order by
    *     revenue desc,
    *     o_orderdate;
    * }}}
    */
  def query03(customers: Seq[Customer], orders: Seq[Order], lineitems: Seq[Lineitem]) = {

    if (debug) println("executing query03")

    val filteredCustomers = customers.filter(_.mktSegment == "BUILDING")
    val filteredOrders = orders.filter(_.orderDate < "1995-03-15")

    val custJoinOrders = if (filteredCustomers.size < filteredOrders.size) {
      val createRes = (c: Customer, o: Order) => (o.orderKey, o.orderDate, o.shipPriority)
      hashJoin(filteredCustomers, (c: Customer) => c.custKey, filteredOrders, (o: Order) => o.custKey, createRes)
    } else {
      val createRes = (o: Order, c: Customer) => (o.orderKey, o.orderDate, o.shipPriority)
      hashJoin(filteredOrders, (o: Order) => o.custKey, filteredCustomers, (c: Customer) => c.custKey, createRes)
    }

    val filteredLineitems = lineitems.filter(_.shipDate > "1995-03-15")

    val join = if (custJoinOrders.size < filteredLineitems.size) {
      val createRes = (j: (Int, String, Int), l: Lineitem) => Query03Schema.Join(l.orderKey,
                                                                                 l.extendedPrice,
                                                                                 l.discount,
                                                                                 j._2,
                                                                                 j._3)
      hashJoin(custJoinOrders, (j: (Int, String, Int)) => j._1, filteredLineitems, (l: Lineitem) => l.orderKey,
               createRes)
    } else {
      val createRes = (l: Lineitem, j: (Int, String, Int)) => Query03Schema.Join(l.orderKey,
                                                                                 l.extendedPrice,
                                                                                 l.discount,
                                                                                 j._2,
                                                                                 j._3)
      hashJoin(filteredLineitems, (l: Lineitem) => l.orderKey, custJoinOrders, (j: (Int, String, Int)) => j._1,
               createRes)
    }

    if (debug) println("computed join for query03")

    // aggregate and compute the final result
    val result = for (
      g <- group(join, (x: Query03Schema.Join) => new Query03Schema.GrpKey(x.orderKey,
                                                                           x.orderDate,
                                                                           x.shipPriority)))
      yield {
        new Query03Schema.Result(g.key.orderKey,
                                 g.values.map(x => x.extendedPrice * (1 - x.discount)).sum,
                                 g.key.orderDate,
                                 g.key.shipPriority)
      }

    val sorted = result.sortBy(r => (-r.revenue, r.orderDate))

    if (debug) println(s"Query03 Results: $sorted")

    sorted
  }

  def hashJoin[A, B, Key, Result](left: Seq[A], keyLeft: A => Key, right: Seq[B], keyRight: B => Key,
                                  createResult: (A, B) => Result): Seq[Result] = {
    //create hash map for left sequence
    var hashMap = Map[Key, ListBuffer[A]]()
    for (l <- left) {
      val key = keyLeft(l)
      if (hashMap isDefinedAt key) {
        var values = hashMap(key)
        values += l
        hashMap += (key -> values)
      } else {
        hashMap += (key -> ListBuffer[A](l))
      }
    }

    val res = ListBuffer[Result]()
    //traverse right sequence and find matching partner in hash map
    for (r <- right) {
      val rightKey = keyRight(r)
      if (hashMap isDefinedAt rightKey) {
        val matchedValues = hashMap(rightKey)
        for (value <- matchedValues) {
          res += createResult(value, r)
        }
      }
    }

    res.toList
  }

  def group[A, K](vals: Seq[A], k: (A) => K): Seq[Group[K, Seq[A]]] =
    vals.groupBy(k).toSeq.map { case (k, v) => Group(k, v) }

  /**
    * Original query:
    *
    * {{{
    * select
    *    n_name,
    *    sum(l_extendedprice * (1 - l_discount)) as revenue
    * from
    *    customer,
    *    orders,
    *    lineitem,
    *    supplier,
    *    nation,
    *    region
    * where
    *    c_custkey = o_custkey
    *    and l_orderkey = o_orderkey
    *    and l_suppkey = s_suppkey
    *    and c_nationkey = s_nationkey
    *    and s_nationkey = n_nationkey
    *    and n_regionkey = r_regionkey
    *    and r_name = '[REGION]'
    *    and o_orderdate >= date '[DATE]'
    *    and o_orderdate < date '[DATE]' + interval '1' year
    * group by
    *    n_name
    * order by
    *    revenue desc;
    * }}}
    */
  def query05(customers: Seq[Customer],
              orders: Seq[Order],
              lineitems: Seq[Lineitem],
              suppliers: Seq[Supplier],
              nations: Seq[Nation],
              regions: Seq[Region]) = {

    if (debug) println("executing query05")

    val dfm = new SimpleDateFormat("yyyy-MM-dd")
    val cal = Calendar.getInstance()
    cal.setTime(dfm.parse("1994-01-01"))
    cal.add(Calendar.YEAR,
            1)
    val nextYear = dfm.format(cal.getTime)

    val filteredOrders = orders.filter(o => o.orderDate >= "1994-01-01" && o.orderDate < nextYear)

    val custJoinOrders = if (customers.size < filteredOrders.size) {
      val createRes = (c: Customer, o: Order) => (o.orderKey, c.nationKey)
      hashJoin(customers, (c: Customer) => c.custKey, filteredOrders, (o: Order) => o.custKey, createRes)
    } else {
      val createRes = (o: Order, c: Customer) => (o.orderKey, c.nationKey)
      hashJoin(filteredOrders, (o: Order) => o.custKey, customers, (c: Customer) => c.custKey, createRes)
    }

    val custOrdersJoinLineitems = if (custJoinOrders.size < lineitems.size) {
      val createRes = (co: (Int, Int), l: Lineitem) => (co._2, l.suppKey, l.extendedPrice, l.discount)
      hashJoin(custJoinOrders, (co: (Int, Int)) => co._1, lineitems, (l: Lineitem) => l.orderKey, createRes)
    } else {
      val createRes = (l: Lineitem, co: (Int, Int)) => (co._2, l.suppKey, l.extendedPrice, l.discount)
      hashJoin(lineitems, (l: Lineitem) => l.orderKey, custJoinOrders, (co: (Int, Int)) => co._1, createRes)
    }

    val custOrdersLitemsJoinSupp = if (custOrdersJoinLineitems.size < suppliers.size) {
      val createRes = (col: (Int, Int, Double, Double), s: Supplier) => (col._1, col._3, col._4, s.nationKey)
      hashJoin(custOrdersJoinLineitems, (col: (Int, Int, Double, Double)) => col._2, suppliers,
               (s: Supplier) => s.suppKey, createRes)
    } else {
      val createRes = (s: Supplier, col: (Int, Int, Double, Double)) => (col._1, col._3, col._4, s.nationKey)
      hashJoin(suppliers, (s: Supplier) => s.suppKey, custOrdersJoinLineitems,
               (col: (Int, Int, Double, Double)) => col._2, createRes)
    }

    val filteredCustOrdersLitemsSupp = custOrdersLitemsJoinSupp.filter(cols => cols._1 == cols._4)

    val custOrdersLitemsSuppJoinNation = if (filteredCustOrdersLitemsSupp.size < nations.size) {
      val createRes = (cols: (Int, Double, Double, Int), n: Nation) => (n.regionKey, cols._2, cols._3,
        n.name)
      hashJoin(filteredCustOrdersLitemsSupp, (cols: (Int, Double, Double, Int)) => cols._1, nations,
               (n: Nation) => n.nationKey, createRes)
    } else {
      val createRes = (n: Nation, cols: (Int, Double, Double, Int)) => (n.regionKey, cols._2, cols._3,
        n.name)
      hashJoin(nations, (n: Nation) => n.nationKey, filteredCustOrdersLitemsSupp, (cols: (Int, Double, Double, Int))
      => cols._1, createRes)
    }

    val filteredRegions = regions.filter(r => r.name == "AMERICA")

    val join = if (custOrdersLitemsSuppJoinNation.size < filteredRegions.size) {
      val createRes = (colsn: (Int, Double, Double, String), r: Region) => Query05Schema
                                                                           .Join(colsn._4, colsn._2, colsn._3)
      hashJoin(custOrdersLitemsSuppJoinNation, (colsn: (Int, Double, Double, String)) => colsn._1, filteredRegions,
               (r: Region) => r.regionKey, createRes)
    } else {
      val createRes = (r: Region, colsn: (Int, Double, Double, String)) => Query05Schema
                                                                           .Join(colsn._4, colsn._2, colsn._3)
      hashJoin(filteredRegions, (r: Region) => r.regionKey, custOrdersLitemsSuppJoinNation, (colsn: (Int, Double,
        Double, String)) => colsn._1, createRes)
    }

    // compute join part of the query
    /*
    val join = for {
      c <- customers
      o <- orders
      if o.orderDate >= "1994-01-01"
      if o.orderDate < nextYear
      if c.custKey == o.custKey
      l <- lineitems
      if l.orderKey == o.orderKey
      s <- suppliers
      if l.suppKey == s.suppKey
      n <- nations
      if c.nationKey == s.nationKey
      if s.nationKey == n.nationKey
      r <- regions
      if r.name == "AMERICA"
      if n.regionKey == r.regionKey
    } yield Query05Schema.Join(n.name,
                               l.extendedPrice,
                               l.discount)
                               */

    if (debug) println("computed join for query05")

    // aggregate and compute the final result
    val result = for {
      g <- group(join, (x: Query05Schema.Join) => new Query05Schema.GrpKey(x.name))
    } yield Query05Schema.Result(g.key.name, g.values.map(x => x.extendedPrice * (1 - x.discount)).sum)

    val sorted = result.sortBy(r => -r.revenue)

    if (debug) println(s"Query05 Results: $sorted")

    sorted
  }

  /**
    * Original query:
    *
    * {{{
    * select
    *    sum(l_extendedprice*l_discount) as revenue
    * from
    *    lineitem
    *  where
    *    l_shipdate >= date '[DATE]'
    *    and l_shipdate < date '[DATE]' + interval '1' year
    *    and l_discount between [DISCOUNT] - 0.01 and [DISCOUNT] + 0.01
    *    and l_quantity < [QUANTITY];
    * }}}
    */
  def query06(lineitems: Seq[Lineitem]) = {

    if (debug) println("executing query06")

    val df = new SimpleDateFormat("yyyy-MM-dd")
    val nextYear = df.format(DateUtils.addYears(df.parse("1994-01-01"),
                                                1))

    val select = for {
      l <- lineitems
      if l.shipDate >= "1994-01-01"
      if l.shipDate < nextYear
      if l.discount >= (0.06 - 0.01)
      if l.discount <= (0.06 + 0.01)
      if l.quantity < 24
    } yield
      new Query06Schema.Select(l.extendedPrice,
                               l.discount)

    if (debug) println("computed filter for query06")

    // aggregate and compute the final result
    val result = for {
      g <- group(select, (x: Query06Schema.Select) => new Query06Schema.GrpKey("*"))
    } yield Query06Schema.Result(
                                  g.values.map(x => x.extendedPrice * x.discount).sum)

    if (debug) println(s"Query06 Results: $result")

    result
  }

  def kmeans(points: Seq[Point], cntrds: Seq[Point], epsilon: Double) = {
    var centroids = cntrds
    var iterations = 0
    var change = 0.0
    var solution = Seq.empty[Point]
    
    do {
      // calculate nearest centroids
      solution = for (p <- points) yield {
        val closestCentroid = centroids.minBy(c => scala.math.sqrt(scala.math.pow(p.x - c.x, 2) +
                                                                   scala.math.pow(p.y - c.y, 2)))

        Point(closestCentroid.cid, p.x, p.y)
      }

      // update means
      val newMeans = for (cluster <- group(solution, (p: Point) => p.cid)) yield {
        val sum = cluster.values.foldLeft(Point(cluster.key, 0, 0))((total, curr) =>
                                                                      Point(cluster.key, total.x + curr.x,
                                                                            total.y + curr.y))
        val cnt = cluster.values.size.toDouble
        Point(cluster.key, sum.x / cnt, sum.y / cnt)
      }

      // compute change between the old and the new means
      change = (for {
        mean <- centroids
        newMean <- newMeans
        if mean.cid == newMean.cid
      } yield scala.math.sqrt(scala.math.pow(mean.x - newMean.x, 2) + scala.math.pow(mean.y - newMean.y, 2))).sum

      // use new means for the next iteration
      centroids = newMeans
      
      iterations += 1
    } while (change >= epsilon)

    if (debug) println(s"$iterations iterations to generate K-Means Results: $solution")
    
    solution
  }

  case class Point(cid: Int, x: Double, y: Double)

  object Query01Schema {

    case class GrpKey(returnFlag: String,
                      lineStatus: String)

    case class Result(returnFlag: String,
                      lineStatus: String,
                      sumQty: Int,
                      sumBasePrice: Double,
                      sumDiscPrice: Double,
                      sumCharge: Double,
                      avgQty: Double,
                      avgPrice: Double,
                      avgDisc: Double,
                      countOrder: Long)

  }

  object Query03Schema {

    case class GrpKey(orderKey: Int,
                      orderDate: String,
                      shipPriority: Int)

    case class Join(orderKey: Int,
                    extendedPrice: Double,
                    discount: Double,
                    orderDate: String,
                    shipPriority: Int)

    case class Result(orderKey: Int,
                      revenue: Double,
                      orderDate: String,
                      shipPriority: Int)

  }

  object Query05Schema {

    case class GrpKey(name: String)

    case class Join(name: String,
                    extendedPrice: Double,
                    discount: Double)

    case class Result(name: String,
                      revenue: Double)

  }

  object Query06Schema {

    case class GrpKey(name: String)

    case class Select(extendedPrice: Double,
                      discount: Double)

    case class Result(revenue: Double)

  }

  object Query16Schema {

    case class GrpKey(nationKey: Int)

    case class Join(key: Int,
                    value: Double)

    case class Result(key: Int,
                      value: Double)

  }


  case class Nation(nationKey: Int,
                    name: String,
                    regionKey: Int,
                    comment: String)

  case class Region(regionKey: Int,
                    name: String,
                    comment: String)

  case class Part(partKey: Int,
                  name: String,
                  mfgr: String,
                  brand: String,
                  ptype: String,
                  size: Int,
                  container: String,
                  retailPrice: Double,
                  comment: String)

  case class Supplier(suppKey: Int,
                      name: String,
                      address: String,
                      nationKey: Int,
                      phone: String,
                      accBal: Double,
                      comment: String)

  case class PartSupp(partKey: Int,
                      suppKey: Int,
                      availQty: Int,
                      supplyCost: Double,
                      comment: String)

  case class Customer(custKey: Int,
                      name: String,
                      address: String,
                      nationKey: Int,
                      phone: String,
                      accBal: Double,
                      mktSegment: String,
                      comment: String)

  case class Order(orderKey: Int,
                   custKey: Int,
                   orderStatus: String,
                   totalPrice: Double,
                   orderDate: String,
                   orderPriority: String,
                   clerk: String,
                   shipPriority: Int,
                   comment: String)

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
