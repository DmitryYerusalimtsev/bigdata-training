package com.epam.hubd.spark.scala.core.homework

import java.math.MathContext

import com.epam.hubd.spark.scala.core.homework.domain._
import org.apache.spark.rdd.RDD
import org.apache.spark.{SparkConf, SparkContext}

object MotelsHomeRecommendation {

  val ERRONEOUS_DIR: String = "erroneous"
  val AGGREGATED_DIR: String = "aggregated"

  //val isIDE = ManagementFactory.getRuntimeMXBean.getInputArguments.toString.contains("IntelliJ IDEA")

  def main(args: Array[String]): Unit = {
    require(args.length == 4, "Provide parameters in this order: bidsPath, motelsPath, exchangeRatesPath, outputBasePath")

    val bidsPath = args(0)
    val motelsPath = args(1)
    val exchangeRatesPath = args(2)
    val outputBasePath = args(3)

    val sc = new SparkContext(getSparkConfiguration())

    processData(sc, bidsPath, motelsPath, exchangeRatesPath, outputBasePath)

    sc.stop()
  }

  private def getSparkConfiguration(): SparkConf = {
    val conf = new SparkConf()
      .setAppName("motels-home-recommendation")

    System.setProperty("hadoop.home.dir", "C:\\Libraries\\WinUtils")
    conf.setMaster("local[*]")

    conf
  }

  def processData(sc: SparkContext, bidsPath: String, motelsPath: String, exchangeRatesPath: String, outputBasePath: String) = {

    /**
      * Task 1:
      * Read the bid data from the provided file.
      */
    val rawBids: RDD[List[String]] = getRawBids(sc, bidsPath)

    /**
      * Task 1:
      * Collect the errors and save the result.
      * Hint: Use the BideError case class
      */
    val erroneousRecords: RDD[String] = getErroneousRecords(sc, rawBids)
    erroneousRecords.saveAsTextFile(s"$outputBasePath/$ERRONEOUS_DIR")

    /**
      * Task 2:
      * Read the exchange rate information.
      * Hint: You will need a mapping between a date/time and rate
      */
    val exchangeRates: Map[String, Double] = getExchangeRates(sc, exchangeRatesPath)

    /**
      * Task 3:
      * Transform the rawBids and use the BidItem case class.
      * - Convert USD to EUR. The result should be rounded to 3 decimal precision.
      * - Convert dates to proper format - use formats in Constants util class
      * - Get rid of records where there is no price for a Losa or the price is not a proper decimal number
      */
    val bids: RDD[BidItem] = getBids(rawBids, exchangeRates)

    /**
      * Task 4:
      * Load motels data.
      * Hint: You will need the motels name for enrichment and you will use the id for join
      */
    val motels: RDD[(String, String)] = getMotels(sc, motelsPath)

    /**
      * Task5:
      * Join the bids with motel names and utilize EnrichedItem case class.
      * Hint: When determining the maximum if the same price appears twice then keep the first entity you found
      * with the given price.
      */
    val enriched: RDD[EnrichedItem] = getEnriched(bids, motels)
    enriched.saveAsTextFile(s"$outputBasePath/$AGGREGATED_DIR")
  }

  def getRawBids(sc: SparkContext, bidsPath: String): RDD[List[String]] = {
    readRDD(sc, bidsPath)
  }

  def getErroneousRecords(sc: SparkContext, rawBids: RDD[List[String]]): RDD[String] = {

    val dateIdx = Constants.BIDS_HEADER.indexOf("BidDate")

    val errorsRDD = rawBids.filter(bid => bid(2).startsWith("ERROR_"))
      .map(bid => BidError(bid(dateIdx), bid(2)))

    val errorsCountedRDD = errorsRDD.countByValue()
    val result = errorsCountedRDD.map(raw => s"${raw._1.toString},${raw._2}\r\n")
    val sortedResult = result.toSeq.sortWith(_ < _)
    sc.parallelize(sortedResult)
  }

  def getExchangeRates(sc: SparkContext, exchangeRatesPath: String): Map[String, Double] = {
    val initialRDD = readRDD(sc, exchangeRatesPath)

    val validFromIdx = Constants.EXCHANGE_RATES_HEADER.indexOf("ValidFrom")
    val excRateIdx = Constants.EXCHANGE_RATES_HEADER.indexOf("ExchangeRate")

    val map = initialRDD.map({
      r => (new String(r(validFromIdx)), r(excRateIdx).toDouble)
    }).collectAsMap().toMap

    map
  }

  def getBids(rawBids: RDD[List[String]], exchangeRates: Map[String, Double]): RDD[BidItem] = {
    val idIdx = Constants.BIDS_HEADER.indexOf("MotelID")
    val dateIdx = Constants.BIDS_HEADER.indexOf("BidDate")
    val usIdx = Constants.BIDS_HEADER.indexOf("US")
    val caIdx = Constants.BIDS_HEADER.indexOf("CA")
    val mxIdx = Constants.BIDS_HEADER.indexOf("MX")

    val focusRDD = rawBids.filter(bid => !bid(2).startsWith("ERROR_"))
      .map(r => List(r(idIdx), r(dateIdx), r(usIdx), r(caIdx), r(mxIdx)))

    val bidsListRDD = focusRDD.map({ r =>
      val rate = exchangeRates(r(1))

      def create = createBidItem(r(0), r(1), rate) _

      List(
        create("US", r(2)),
        create("CA", r(3)),
        create("MX", r(4))
      ).flatten
    })

    bidsListRDD.flatMap(r => r)
  }

  def getMotels(sc: SparkContext, motelsPath: String): RDD[(String, String)] = {
    val initialRDD = readRDD(sc, motelsPath)

    val idIdx = Constants.MOTELS_HEADER.indexOf("MotelID")
    val nameIdx = Constants.MOTELS_HEADER.indexOf("MotelName")

    initialRDD.map(r => (r(idIdx), r(nameIdx)))
  }

  def getEnriched(bids: RDD[BidItem], motels: RDD[(String, String)]): RDD[EnrichedItem] = {
    val keyedBidsRDD = bids.map(b => (b.motelId, b))
    val joinedRDD = keyedBidsRDD.join(motels)

    joinedRDD.map(r => {
      val id = r._1
      val name = r._2._2
      val bid = r._2._1
      EnrichedItem(id, name, bid.bidDate, bid.loSa, bid.price)
    })
  }

  // Private helpers.

  private def readRDD(sc: SparkContext, path: String): RDD[List[String]] = {
    sc.textFile(path)
      .map(s => s.split(Constants.DELIMITER).toList)
  }

  private def createBidItem(id: String, date: String, rate: Double)
                           (loSa: String, price: String): Option[BidItem] = {

    val formattedDate = convertDateFormat(date)

    !isEmpty(price) match {
      case true => {
        val exchanged = price.toDouble * rate
        val bd = BigDecimal(exchanged)
        val rounded = bd.round(new MathContext(3)).doubleValue()

        Some(BidItem(id, formattedDate, loSa, rounded))
      }
      case false => None
    }
  }

  private def convertDateFormat(date: String): String = {
    val dt = Constants.INPUT_DATE_FORMAT.parseDateTime(date)
    Constants.OUTPUT_DATE_FORMAT.print(dt)
  }
}
