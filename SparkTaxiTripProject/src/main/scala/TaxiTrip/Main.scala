package TaxiTrip

import org.apache.spark
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.spark.sql.Row
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming.Trigger
import scala.concurrent.duration._
import org.apache.spark.sql.catalyst.expressions._
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.functions._
import org.apache.spark.sql.functions.{ concat, lit }
import java.sql.Timestamp
import org.apache.spark.sql.Column
import org.apache.spark.sql._

object Main {

  def main(args: Array[String]): Unit = {

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("ProcessData")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    val path = new Common()

    /*Capturing the below to calculate the delay column for the final output*/
    spark.sql("""select date_format(current_timestamp(),"yyyy-MM-dd HH:mm:ss.SSS") """).write.mode("overwrite").csv(path.OuputAuditPath)

    val initDataProcessing = new DataProcessing()

    val initFrequentRoutes = new FrequentRoutes()

    val processedData = initDataProcessing.ProcessedData()

    initFrequentRoutes.GetTop10Routes(processedData)

  }
}