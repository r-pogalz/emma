import java.text.SimpleDateFormat
import java.util.Calendar
import java.util.concurrent.TimeUnit

import eu.stratosphere.emma.api.{CSVInputFormat, _}
import org.apache.commons.lang.time.DateUtils

object BenchmarkTPCH {

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
          toOptionMap(map ++ Map(pathSym -> string),
                      list.tail)
        case string :: Nil => toOptionMap(map ++ Map(pathSym -> string),
                                          list.tail)
        case option :: tail => throw new IllegalArgumentException("Unknown option " + option)
      }
    }
    val options = toOptionMap(Map(),
                              arglist)

    debug = options.getOrElse(debugSym,
                              false).asInstanceOf[Boolean]

    val warmupRnds: Int = options.getOrElse(warmupSym,
                                            defaultWarmup).asInstanceOf[Int]
    val measureRnds: Int = options.getOrElse(roundsSym,
                                             defaultRounds).asInstanceOf[Int]
    val tblPath: String = options.getOrElse(pathSym,
                                            throw new IllegalArgumentException("No path to tbl files specified"))
                          .asInstanceOf[String]

    val lineitems = for {
      l <- read(s"$tblPath/lineitem.tbl",
                new CSVInputFormat[Lineitem]('|'))
    } yield l
    println("read lineitem tbl successfully")

    val partsuppliers = for {
      ps <- read(s"$tblPath/partsupp.tbl",
                 new CSVInputFormat[PartSupp]('|'))
    } yield ps
    println("read partsupp tbl successfully")

    val suppliers = for {
      s <- read(s"$tblPath/supplier.tbl",
                new CSVInputFormat[Supplier]('|'))
    } yield s
    println("read supplier tbl successfully")

    /*
    val customers = for {
      c <- read(s"$tblPath/customer.tbl",
                new CSVInputFormat[Customer]('|'))
    } yield c
    println("read customer tbl successfully")

    val parts = for {
      p <- read(s"$tblPath/part.tbl",
                new CSVInputFormat[Part]('|'))
    } yield p
    println("read part tbl successfully")

    val orders = for {
      o <- read(s"$tblPath/orders.tbl",
                new CSVInputFormat[Order]('|'))
    } yield o
    println("read orders tbl successfully")

    val nations = for {
      n <- read(s"$tblPath/nation.tbl",
                new CSVInputFormat[Nation]('|'))
    } yield n
    println("read nation tbl successfully")

    val regions = for {
      r <- read(s"$tblPath/region.tbl",
                new CSVInputFormat[Region]('|'))
    } yield r
    println("read region tbl successfully")
    */


    //profile query01 from TPC-H
    profile(warmupRnds,
            measureRnds,
            query01(lineitems),
            "TPC-H Query01")

    /*
    //profile query03 from TPC-H
    profile(warmupRnds,
            measureRnds,
            query03(customers,
                    orders,
                    lineitems),
            "TPC-H Query03")

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
    */

    //profile query06 from TPC-H
    profile(warmupRnds,
            measureRnds,
            query06(lineitems),
            "TPC-H Query06")

    //profile join query
    profile(warmupRnds,
            measureRnds,
            joinQuery(partsuppliers,
                      suppliers),
            "TPC-H JoinQuery")
  }

  private def profile[A](warmupRounds: Int,
                         times: Int,
                         query: => DataBag[A],
                         title: String) = {

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
  def query01(lineitems: DataBag[Lineitem]) = {

    if (debug) println("executing query01")

    // aggregate and compute the final result
    val result = for {
      g <- lineitems.withFilter(l => l.shipDate <= "1998-12-01").groupBy(l => new Query01Schema.GrpKey(l.returnFlag,
                                                                                                       l.lineStatus))
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

    if (debug) println(s"Query01 Results: $result")

    result
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
  def query03(customers: DataBag[Customer],
              orders: DataBag[Order],
              lineitems: DataBag[Lineitem]) = {

    if (debug) println("executing query03")

    // compute join part of the query
    val join = for {
      c <- customers
      if c.mktSegment == "AUTOMOBILE"
      o <- orders
      if o.orderDate < "1996-06-30"
      if c.custKey == o.custKey
      l <- lineitems
      if l.shipDate > "1996-06-30"
      if l.orderKey == o.orderKey
    } yield Query03Schema.Join(l.orderKey,
                               l.extendedPrice,
                               l.discount,
                               o.orderDate,
                               o.shipPriority)

    if (debug) println("computed join for query03")

    // aggregate and compute the final result
    val result = for (
      g <- join.groupBy(x => new Query03Schema.GrpKey(x.orderKey,
                                                      x.orderDate,
                                                      x.shipPriority)))
      yield {
        new Query03Schema.Result(g.key.orderKey,
                                 g.values.map(x => x.extendedPrice * (1 - x.discount)).sum,
                                 g.key.orderDate,
                                 g.key.shipPriority)
      }

    if (debug) println(s"Query03 Results: $result")

    result
  }

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
  def query05(customers: DataBag[Customer],
              orders: DataBag[Order],
              lineitems: DataBag[Lineitem],
              suppliers: DataBag[Supplier],
              nations: DataBag[Nation],
              regions: DataBag[Region]) = {

    if (debug) println("executing query05")

    val dfm = new SimpleDateFormat("yyyy-MM-dd")
    val cal = Calendar.getInstance()
    cal.setTime(dfm.parse("1994-01-01"))
    cal.add(Calendar.YEAR,
            1)
    val nextYear = dfm.format(cal.getTime)

    // compute join part of the query
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

    if (debug) println("computed join for query05")

    // aggregate and compute the final result
    val result = for {
      g <- join.groupBy(x => new Query05Schema.GrpKey(x.name))
    } yield Query05Schema.Result(
                                  g.key.name,
                                  g.values.map(x => x.extendedPrice * (1 - x.discount)).sum)

    if (debug) println(s"Query05 Results: $result")

    result
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
  def query06(lineitems: DataBag[Lineitem]) = {

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
      g <- select.groupBy(x => new Query06Schema.GrpKey("*"))
    } yield Query06Schema.Result(
                                  g.values.map(x => x.extendedPrice * x.discount).sum)

    if (debug) println(s"Query06 Results: $result")

    result
  }

  /**
    * Original query:
    *
    * {{{
    * select
    *    s_nationkey,
    *    sum(ps_supplycost)
    * from
    *    partsupp,
    *    supplier
    * where
    *    s_suppkey = ps_suppkey
    * group by
    *    s_nationkey
    * }}}
    */
  def joinQuery(partsuppliers: DataBag[PartSupp],
                suppliers: DataBag[Supplier]) = {

    if (debug) println("executing join query")

    //compute join part
    var i = 0
    val join = for {
      ps <- partsuppliers
      s <- suppliers
      if (ps.suppKey == s.suppKey)
    } yield {
      Query16Schema.Join(s.nationKey,
                         ps.supplyCost)
    }

    if (debug) println("computed join for query16")

    // aggregate and compute the final result
    val result = for {
      g <- join.groupBy(x => new Query16Schema.GrpKey(x.key))
    } yield Query16Schema.Result(
                                  g.key.nationKey,
                                  g.values.map(j => j.value).sum)

    if (debug) println(s"JoinQuery Results: $result")

    result
  }

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
