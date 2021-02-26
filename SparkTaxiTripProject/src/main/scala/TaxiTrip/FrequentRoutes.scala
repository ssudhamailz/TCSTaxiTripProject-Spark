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

class FrequentRoutes {

  def GetTop10Routes(processeddata: DataFrame) {

    val common = new Common()
    
    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("ProcessData")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    import spark.sqlContext.implicits._
    
      var pickup_datetime = ""
      var dropoff_datetime = ""
      val counter = 10
      var nullrecords = ""
      var result = ""

    //To find the cell 1x1
    val First_Cell_Latitude = common.CenterLatitude + common.LATITUDE / 2 
    val First_Cell_Longitude = common.CenterLongitude - common.LONGITUDE / 2
    
    //To find the cell 300x300
    val Last_Cell_Latitude = First_Cell_Latitude - (300 * common.LATITUDE)
    val Last_Cell_Longitude = First_Cell_Longitude + (300 * common.LONGITUDE)

    val finaldata = processeddata
      .where($"pickup_latitude" >= Last_Cell_Latitude).where($"pickup_latitude" <= First_Cell_Latitude)
      .where($"dropoff_latitude" >= Last_Cell_Latitude).where($"dropoff_latitude" <= First_Cell_Latitude)
      .where($"pickup_longitude" >= First_Cell_Longitude).where($"pickup_longitude" <= Last_Cell_Longitude)
      .where($"dropoff_longitude" >= First_Cell_Longitude).where($"dropoff_longitude" <= Last_Cell_Longitude)

    //finaldata.repartition(1).write.mode("overwrite").csv("file:///D:/TCS/processeddata")

    /*Find the top 10 most frequent routes during the last 30 minutes */

/**************************************For Live Streaming**********************************************************/
    val getEndTime = spark.sql("""select current_timestamp() reportendtime""").selectExpr("reportendtime").rdd.map(x => x.mkString).collect
    val getStartTime = spark.sql("""select current_timestamp() - INTERVAL 30 minutes as reportstarttime""").selectExpr("reportstarttime").rdd.map(x => x.mkString).collect

/**************************************For Running against dataset repository**********************************************************/
    val dropoff_latesttime = finaldata.orderBy($"dropoff_datetime".desc).select("dropoff_datetime").limit(1)

    val filterdate = dropoff_latesttime
      .withColumn("reportstarttime", col("dropoff_datetime") - expr("INTERVAL 30 minutes"))
      .withColumn("reportendtime", col("dropoff_datetime"))

    val reportstarttime = filterdate.selectExpr("reportstarttime").rdd.map(x => x.mkString).collect
    val reportendtime = filterdate.selectExpr("reportendtime").rdd.map(x => x.mkString).collect
/************************************************************************************************/
        
    if (common.IsLiveStreaming == "Y") {
      pickup_datetime = getStartTime(0)
      dropoff_datetime = getEndTime(0)
    } else {
      pickup_datetime = reportstarttime(0)
      dropoff_datetime = reportendtime(0)
    }

    val frequentroute = finaldata
      .filter($"pickup_datetime" >= pickup_datetime)
      .filter($"pickup_datetime" <= dropoff_datetime)
      .filter($"dropoff_datetime" >= pickup_datetime)
      .filter($"dropoff_datetime" <= dropoff_datetime)
      .groupBy(col("pickup_longitude"), col("pickup_latitude"), col("dropoff_longitude"), col("dropoff_latitude"))
      .count().orderBy(desc("count"))
      .select(concat($"pickup_latitude", lit(" "), $"pickup_longitude").alias("startcell"), concat($"dropoff_latitude", lit(" "), $"dropoff_longitude").alias("endcell"))
      .limit(10)

  
    var diff = counter - frequentroute.count().toInt    

    val top10record = frequentroute.map(row => row.mkString("|")).select("value").collect().map(_.getString(0)).mkString("|")

    val readaudit = spark.read.csv(common.OuputAuditPath).toDF("JobStartTime")

    val jobduration = readaudit.withColumn("DiffInSeconds", current_timestamp().cast(LongType) - to_timestamp(col("JobStartTime")).cast(LongType))

    val delay = jobduration.select(col("DiffInSeconds")).map(row => row.mkString("|")).select("value").collect().map(_.getString(0)).mkString("|")

    if (diff <= counter && diff != 0) {

      if (diff == counter) {

        result = pickup_datetime + "|" + dropoff_datetime + "|" + common.writenull(diff, counter) + "|" + delay
      } else {
        result = pickup_datetime + "|" + dropoff_datetime + "|" + top10record + "|" + common.writenull(diff, counter) + "|" + delay
      }
    } else {
      result = pickup_datetime + "|" + dropoff_datetime + "|" + top10record + "|" + delay
    }

    val formattedoutput = Seq(result).toDF("output")

    formattedoutput.write.mode("overwrite").text(common.FormattedOutputPath)

    val output = spark.read.option("delimiter", "|").csv(common.FormattedOutputPath).toDF("pickup_datetime", "dropoff_datetime", "start_cell_id_1", "end_cell_id_1",
      "start_cell_id_2", "end_cell_id_2", "start_cell_id_3", "end_cell_id_3", "start_cell_id_4", "end_cell_id_4",
      "start_cell_id_5", "end_cell_id_5", "start_cell_id_6", "end_cell_id_6", "start_cell_id_7", "end_cell_id_7",
      "start_cell_id_8", "end_cell_id_8", "start_cell_id_9", "end_cell_id_9", "start_cell_id_10", "end_cell_id_10", "delay_in_sec")

    //output.write.option("header","true").mode("overwrite").mode(SaveMode.Append).csv("file:///D:/TCS/OUTPUT")
    output.write.option("header", "true").mode("overwrite").csv(common.FinalOutputPath)

  }

}