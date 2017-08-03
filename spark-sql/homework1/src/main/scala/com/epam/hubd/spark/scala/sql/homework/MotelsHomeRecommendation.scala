package com.epam.hubd.spark.scala.sql.homework

import java.math.MathContext

import org.apache.spark.sql.{DataFrame, SQLContext, UserDefinedFunction}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._

object MotelsHomeRecommendation {

  val ERRONEOUS_DIR: String = "erroneous"
  val AGGREGATED_DIR: String = "aggregated"

  def main(args: Array[String]): Unit = {
    require(args.length == 4, "Provide parameters in this order: bidsPath, motelsPath, exchangeRatesPath, outputBasePath")

    val bidsPath = args(0)
    val motelsPath = args(1)
    val exchangeRatesPath = args(2)
    val outputBasePath = args(3)

    val sc = new SparkContext(
      new SparkConf()
        .setAppName("motels-home-recommendation")
        .setMaster("local[*]"))

    implicit val sqlContext = new SQLContext(sc)

    processData(bidsPath, motelsPath, exchangeRatesPath, outputBasePath)

    sc.stop()
  }

  def processData(bidsPath: String, motelsPath: String, exchangeRatesPath: String, outputBasePath: String)
                 (implicit sqlContext: SQLContext): Unit = {

    /**
      * Task 1:
      * Read the bid data from the provided file.
      */
    val rawBids: DataFrame = getRawBids(bidsPath)

    /**
      * Task 1:
      * Collect the errors and save the result.
      */
    val erroneousRecords: DataFrame = getErroneousRecords(rawBids)
    erroneousRecords.show(10)
    erroneousRecords.write
      .format(Constants.CSV_FORMAT)
      .save(s"$outputBasePath/$ERRONEOUS_DIR")

    /**
      * Task 2:
      * Read the exchange rate information.
      * Hint: You will need a mapping between a date/time and rate
      */
    val exchangeRates: DataFrame = getExchangeRates(exchangeRatesPath)
    exchangeRates.show(10)

    /**
      * Task 3:
      * UserDefinedFunction to convert between date formats.
      * Hint: Check the formats defined in Constants class
      */
    val convertDate: UserDefinedFunction = getConvertDate

    /**
      * Task 3:
      * Transform the rawBids
      * - Convert USD to EUR. The result should be rounded to 3 decimal precision.
      * - Convert dates to proper format - use formats in Constants util class
      * - Get rid of records where there is no price for a Losa or the price is not a proper decimal number
      */
    val bids: DataFrame = getBids(rawBids, exchangeRates, convertDate)
    bids.show(10)

    /**
      * Task 4:
      * Load motels data.
      * Hint: You will need the motels name for enrichment and you will use the id for join
      */
    val motels: DataFrame = getMotels(motelsPath)
    motels.show(10)

    /**
      * Task5:
      * Join the bids with motel names.
      */
    val enriched: DataFrame = getEnriched(bids, motels)
    enriched.show(10)
    enriched.write
      .format(Constants.CSV_FORMAT)
      .save(s"$outputBasePath/$AGGREGATED_DIR")
  }

  def getRawBids(bidsPath: String)
                (implicit sqlContext: SQLContext): DataFrame = {
    sqlContext.read
      .format(Constants.CSV_FORMAT)
      .schema(Constants.RAW_BIDS_HEADER)
      .load(bidsPath)
  }

  def getErroneousRecords(rawBids: DataFrame)
                         (implicit sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._

    val errorsDF = rawBids.filter($"HU".startsWith("ERROR_"))
    errorsDF.show(10)

    errorsDF.groupBy($"BidDate", $"HU")
      .agg(count("*").alias("Count"))
      .select($"BidDate", $"HU".alias("Error"), $"Count")
  }

  def getExchangeRates(exchangeRatesPath: String)
                      (implicit sqlContext: SQLContext): DataFrame = {
    sqlContext.read
      .format(Constants.CSV_FORMAT)
      .schema(Constants.EXCHANGE_RATES_HEADER)
      .load(exchangeRatesPath)
  }

  def getConvertDate: UserDefinedFunction = {
    val func = (date: String) => {
      val dt = Constants.INPUT_DATE_FORMAT.parseDateTime(date)
      Constants.OUTPUT_DATE_FORMAT.print(dt)
    }

    udf(func)
  }

  def getBids(rawBids: DataFrame, exchangeRates: DataFrame, convertDate: UserDefinedFunction)
             (implicit sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._
    val bidsDf = rawBids.filter(!$"HU".startsWith("ERROR_"))
      .join(exchangeRates, $"BidDate" === $"ValidFrom")
      .select($"MotelID", $"BidDate", $"US", $"CA", $"MX", $"ExchangeRate")

    getBidDF(bidsDf, "US", convertDate)
      .unionAll(getBidDF(bidsDf, "CA", convertDate))
      .unionAll(getBidDF(bidsDf, "MX", convertDate))
  }

  def getMotels(motelsPath: String)
               (implicit sqlContext: SQLContext): DataFrame = {
    sqlContext.read
      .format(Constants.CSV_FORMAT)
      .schema(Constants.MOTELS_HEADER)
      .load(motelsPath)
  }

  def getEnriched(bids: DataFrame, motels: DataFrame)
                 (implicit sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._
    val bidsAlias = bids.as("bids")
    val motelsAlias = motels.as("motels")

    val joinedDF = bidsAlias.join(motelsAlias, col("bids.MotelID") === col("motels.MotelID"))

    joinedDF.select($"motels.MotelID", $"motels.MotelName", $"bids.BidDate", $"bids.LoSa", $"bids.Price")
  }

  // Private methods

  private def getBidDF(bidsDF: DataFrame, loSa: String, convertDate: UserDefinedFunction)
                      (implicit sqlContext: SQLContext): DataFrame = {
    import sqlContext.implicits._

    val df = bidsDF.filter($"$loSa" !== "")

    val convertPrice = udf((price: String, rate: String) => {
      val exchanged = price.toDouble * rate.toDouble
      val bd = BigDecimal(exchanged)
      bd.round(new MathContext(3)).doubleValue()
    })

    df.withColumn("LoSa", lit(loSa))
      .select($"MotelID",
        convertDate($"BidDate").alias("BidDate"),
        $"LoSa",
        convertPrice($"$loSa", $"ExchangeRate").alias("Price"))
  }
}
