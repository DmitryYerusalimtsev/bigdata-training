//package com.epam.hubd.spark.scala.core.homework
//
//import java.io.File
//
//import com.epam.hubd.spark.scala.core.homework.MotelsHomeRecommendation.{AGGREGATED_DIR, ERRONEOUS_DIR}
//import com.epam.hubd.spark.scala.core.homework.MotelsHomeRecommendationTest._
//import com.epam.hubd.spark.scala.core.util.RddComparator
//import com.holdenkarau.spark.testing.RDDComparisons
//import org.apache.hadoop.fs.Path
//import org.apache.spark.{SparkConf, SparkContext}
//import org.junit._
//import org.junit.rules.TemporaryFolder
//
///**
//  * Created by Csaba_Bejan on 8/17/2016.
//  */
//class MotelsHomeRecommendationTest {
//  val _temporaryFolder = new TemporaryFolder
//
//  @Rule
//  def temporaryFolder = _temporaryFolder
//
//  val INPUT_BIDS_SAMPLE = "src/test/resources/bids_sample.txt"
//
//  val INPUT_BIDS_INTEGRATION = "src/test/resources/integration/input/bids.txt"
//  val INPUT_EXCHANGE_RATES_INTEGRATION = "src/test/resources/integration/input/exchange_rate.txt"
//  val INPUT_MOTELS_INTEGRATION = "src/test/resources/integration/input/motels.txt"
//
//  val EXPECTED_AGGREGATED_INTEGRATION = "src/test/resources/integration/expected_output/aggregated"
//  val EXPECTED_ERRORS_INTEGRATION = "src/test/resources/integration/expected_output/error_records"
//
//  private var outputFolder: File = null
//
//  @Before
//  def setup() = {
//    outputFolder = temporaryFolder.newFolder("output")
//  }
//
//  @Test
//  def shouldReadRawBids() = {
//    val expected = sc.parallelize(
//      Seq(
//        List("0000002", "15-04-08-2016", "0.89", "0.92", "1.32", "2.07", "", "1.35"),
//        List("0000001", "06-05-02-2016", "ERROR_NO_BIDS_FOR_HOTEL")
//      )
//    )
//
//    val rawBids = MotelsHomeRecommendation.getRawBids(sc, INPUT_BIDS_SAMPLE)
//
//    RDDComparisons.assertRDDEquals(expected, rawBids)
//  }
//
//  @Test
//  def shouldCollectErroneousRecords() = {
//    val rawBids = sc.parallelize(
//      Seq(
//        List("1", "06-05-02-2016", "ERROR_1"),
//        List("2", "15-04-08-2016", "0.89"),
//        List("3", "07-05-02-2016", "ERROR_2"),
//        List("4", "06-05-02-2016", "ERROR_1"),
//        List("5", "06-05-02-2016", "ERROR_2")
//      )
//    )
//
//    val expected = sc.parallelize(
//      Seq(
//        "06-05-02-2016,ERROR_1,2",
//        "06-05-02-2016,ERROR_2,1",
//        "07-05-02-2016,ERROR_2,1"
//      )
//    )
//
//    val erroneousRecords = MotelsHomeRecommendation.getErroneousRecords(sc, rawBids)
//
//    RDDComparisons.assertRDDEquals(expected, erroneousRecords)
//  }
//
//
//  @Test
//  def shouldFilterErrorsAndCreateCorrectAggregates() = {
//
//    runIntegrationTest()
//
//    //If the test fails and you are interested in what are the differences in the RDDs uncomment the corresponding line
//    //printRddDifferences(EXPECTED_ERRORS_INTEGRATION, getOutputPath(ERRONEOUS_DIR))
//    //printRddDifferences(EXPECTED_AGGREGATED_INTEGRATION, getOutputPath(AGGREGATED_DIR))
//
//    assertRddTextFiles(EXPECTED_ERRORS_INTEGRATION, getOutputPath(ERRONEOUS_DIR))
//    assertRddTextFiles(EXPECTED_AGGREGATED_INTEGRATION, getOutputPath(AGGREGATED_DIR))
//  }
//
//  @After
//  def teardown(): Unit = {
//    outputFolder.delete
//  }
//
//  private def runIntegrationTest() = {
//    MotelsHomeRecommendation.processData(sc, INPUT_BIDS_INTEGRATION, INPUT_MOTELS_INTEGRATION, INPUT_EXCHANGE_RATES_INTEGRATION, outputFolder.getAbsolutePath)
//  }
//
//  private def assertRddTextFiles(expectedPath: String, actualPath: String) = {
//    val expected = sc.textFile(expectedPath)
//    val actual = sc.textFile(actualPath)
//    RDDComparisons.assertRDDEquals(expected, actual)
//  }
//
//  private def printRddDifferences(expectedPath: String, actualPath: String) = {
//    val expected = sc.textFile(expectedPath)
//    val actual = sc.textFile(actualPath)
//    RddComparator.printDiff(expected, actual)
//  }
//
//  private def getOutputPath(dir: String): String = {
//    new Path(outputFolder.getAbsolutePath, dir).toString
//  }
//}
//
//object MotelsHomeRecommendationTest {
//  private var sc: SparkContext = null
//
//  @BeforeClass
//  def beforeTests() = {
//    sc = new SparkContext(new SparkConf().setMaster("local[2]").setAppName("motels-home-recommendation test"))
//  }
//
//  @AfterClass
//  def afterTests() = {
//    sc.stop
//  }
//}
