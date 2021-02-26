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
import  org.apache.spark.sql.Column
import org.apache.spark.sql._

object DataProcessingandFrequentRoutes {
  
    /* def main(args: Array[String]): Unit = {
             
     val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("ProcessData")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")

    import spark.sqlContext.implicits._

    val OuputAuditPath = "file:///D:/Suvitha/Interview2021/TCS/Audit"
    val InputSourceDataPath = "file:///D:/Suvitha/Interview2021/TCS/sorted_data.csv"
    val FormattedOutputPath = "file:///D:/Suvitha/Interview2021/TCS/formattedoutput"
    val FinalOutputPath = "file:///D:/Suvitha/Interview2021/TCS/OUTPUT"
    
    /*Capturing the below to calculate the delay column for the final output*/
    spark.sql("""select date_format(current_timestamp(),"yyyy-MM-dd HH:mm:ss.SSS") """).write.mode("overwrite").csv(OuputAuditPath)

    /*Interpret the inferschema to find  data type from the nature of data*/
    /*val data = spark.read.option("inferSchema",true).csv("file:///D:/tcs/sorted_data.csv").toDF("medallion","hack_license","pickup_datetime","dropoff_datetime","trip_time_in_secs","trip_distance","pickup_longitude","pickup_latitude","dropoff_longitude","dropoff_latitude","payment_type","fare_amount",
  "surcharge","mta_tax","tip_amount","tolls_amount","total_amount")*/

    /*Explicitly passing schema while loading CSV file data in spark dataframe*/
    val trip_schema = StructType(Array(
      StructField("medallion", StringType, true),
      StructField("hack_license", StringType, true),
      StructField("pickup_datetime", TimestampType, true),
      StructField("dropoff_datetime", TimestampType, true),
      StructField("trip_time_in_secs", IntegerType, true),
      StructField("trip_distance", DoubleType, true),
      StructField("pickup_longitude", DoubleType, true),
      StructField("pickup_latitude", DoubleType, true),
      StructField("dropoff_longitude", DoubleType, true),
      StructField("dropoff_latitude", DoubleType, true),
      StructField("payment_type", StringType, true),
      StructField("fare_amount", DoubleType, true),
      StructField("surcharge", DoubleType, true),
      StructField("mta_tax", DoubleType, true),
      StructField("tip_amount", DoubleType, true),
      StructField("tolls_amount", DoubleType, true),
      StructField("total_amount", DoubleType, true)))

    val rawdata = spark.read.schema(trip_schema).csv(InputSourceDataPath)
    //data.printSchema()

    //rawdata.show()

    rawdata.createOrReplaceTempView("vw_trip_rawdata")
    
    //val gridcell = spark.read.csv("file:///D:/Suvitha/Interview2021/TCS/GridMap.csv")
    
     //gridcell.createOrReplaceTempView("vw_gridcell")    

    /*The data is sorted chronologically according to the dropoff_datetime. Events with the same dropoff_datetime are in random order.
		 * Please note that the quality of the data is not perfect. Some events might miss information such as drop off and pickup coordinates
		 * or fare information. Moreover, some information, such as, e.g., the fare price might have been entered incorrectly by the taxi drivers
		 * thus introducing additional skew. Handle accordingly*/

    val processeddata = spark.sql("""select * from vw_trip_rawdata where                                 		  
		                                  pickup_datetime <> dropoff_datetime AND 
                                		  trip_distance > 0 AND total_amount > 0 AND 
                                		  pickup_longitude <> dropoff_longitude AND 
                                		  pickup_latitude <> dropoff_latitude AND 
                                		  pickup_longitude <> 0 AND 
                                		  pickup_latitude <> 0 AND 
                                		  dropoff_longitude <> 0 AND 
                                		  dropoff_latitude <> 0""")

    /*The cells for this query are squares of 500 m X 500 m. The cell grid starts with cell 1.1, located at 41.474937,
     * -74.913585 (in Barryville). The coordinate 41.474937, -74.913585 marks the center of the first cell.
     * Cell numbers increase towards the east and south, with the shift to east being the first and the shift to south the
     * second component of the cell, i.e., cell 3.7 is 2 cells east and 6 cells south of cell 1.1. The overall grid expands
     * 150km south and 150km east from cell 1.1 with the cell 300.300 being the last cell in the grid. All trips starting or
     * ending outside this area are treated as outliers and must not be considered in the result computation.*/

    // 500 m X 500 m value of longitude and latitude per cell are as follows

    //Converting Meter to Decimal Degree

    //Length in meters of 1° of latitude = always 111.32 km
    //Length in meters of 1° of longitude = 40075 km * cos( latitude ) / 360

    val SquareMeter = 500
    val MAPSIZE = 300
    val CenterLatitude = 41.474937
    val CenterLongitude = -74.913585

    val LATITUDE = (SquareMeter / (111.32 * 1000))

    //Using online converter "https://www.fcc.gov/media/radio/distance-and-azimuths" i derive the below longitude
    val LONGITUDE = 0.005681556

    //To find the cell (1,1)

    val First_Cell_Latitude = CenterLatitude + LATITUDE / 2
    val First_Cell_Longitude = CenterLongitude - LONGITUDE / 2
    val Last_Cell_Latitude = First_Cell_Latitude - (300 * LATITUDE)
    val Last_Cell_Longitude = First_Cell_Longitude + (300 * LONGITUDE)

    val finaldata = processeddata
      .where($"pickup_latitude" >= Last_Cell_Latitude).where($"pickup_latitude" <= First_Cell_Latitude)
      .where($"dropoff_latitude" >= Last_Cell_Latitude).where($"dropoff_latitude" <= First_Cell_Latitude)
      .where($"pickup_longitude" >= First_Cell_Longitude).where($"pickup_longitude" <= Last_Cell_Longitude)
      .where($"dropoff_longitude" >= First_Cell_Longitude).where($"dropoff_longitude" <= Last_Cell_Longitude)

    //finaldata.repartition(1).write.mode("overwrite").csv("file:///D:/TCS/processeddata")

    /*Find the top 10 most frequent routes during the last 30 minutes */

/************************************************/
    val currenttime = spark.sql("""select current_timestamp()""")
    val starttime = spark.sql("""select current_timestamp() - INTERVAL 30 minutes as starttime""")

    //currenttime.show(false)
    //starttime.show(false)

/************************************************/
    val dropoff_latesttime = finaldata.orderBy($"dropoff_datetime".desc).select("dropoff_datetime").limit(1)

    val filterdate = dropoff_latesttime
      .withColumn("reportstarttime", col("dropoff_datetime") - expr("INTERVAL 30 minutes"))
      .withColumn("reportendtime", col("dropoff_datetime"))

    val reportstarttime = filterdate.selectExpr("reportstarttime").rdd.map(x => x.mkString).collect
    val reportendtime = filterdate.selectExpr("reportendtime").rdd.map(x => x.mkString).collect
    val pickup_datetime = reportstarttime(0)
    val dropoff_datetime = reportendtime(0)
/************************************************/
    val frequentroute = finaldata
      .filter($"pickup_datetime" >= pickup_datetime)
      .filter($"pickup_datetime" <= dropoff_datetime)
      .filter($"dropoff_datetime" >= pickup_datetime)
      .filter($"dropoff_datetime" <= dropoff_datetime)
      .groupBy(col("pickup_longitude"), col("pickup_latitude"), col("dropoff_longitude"), col("dropoff_latitude"))
      .count().orderBy(desc("count"))
      .select(concat($"pickup_latitude", lit(" "), $"pickup_longitude").alias("startcell"), concat($"dropoff_latitude", lit(" "), $"dropoff_longitude").alias("endcell"))
      .limit(10)
      
      //frequentroute.repartition(1).write.mode("overwrite").csv("file:///D:/Suvitha/Interview2021/TCS/Top10data")
     
     val counter = 10
     var diff = counter-frequentroute.count().toInt
     
     var nullrecords = ""    
     
    val top10record = frequentroute.map(row => row.mkString(",")).select("value").collect().map(_.getString(0)).mkString(",")

    val readaudit = spark.read.csv(OuputAuditPath).toDF("JobStartTime")

    val jobduration = readaudit.withColumn("DiffInSeconds", current_timestamp().cast(LongType) - to_timestamp(col("JobStartTime")).cast(LongType))

    val delay = jobduration.select(col("DiffInSeconds")).map(row => row.mkString(",")).select("value").collect().map(_.getString(0)).mkString(",")

    var result = ""
    
    if(diff <= counter && diff != 0)
    {
      var r = "NULL," * diff * 2
      
      nullrecords = r.substring(0,r.length()-1)
      
      if(diff == counter)
      {
      
      result = pickup_datetime + "," + dropoff_datetime + "," + nullrecords + "," + delay
      }
      else
      {
        result = pickup_datetime + "," + dropoff_datetime + "," + top10record + "," + nullrecords + "," + delay
      }
    }
    else
    {
      result = pickup_datetime + "," + dropoff_datetime + "," + top10record + "," + delay
    }
     
    val formattedoutput = Seq(result).toDF("output")

    formattedoutput.write.mode("overwrite").text(FormattedOutputPath)

    val output = spark.read.option("delimiter", ",").csv(FormattedOutputPath).toDF("pickup_datetime", "dropoff_datetime", "start_cell_id_1", "end_cell_id_1",
      "start_cell_id_2", "end_cell_id_2", "start_cell_id_3", "end_cell_id_3", "start_cell_id_4", "end_cell_id_4",
      "start_cell_id_5", "end_cell_id_5", "start_cell_id_6", "end_cell_id_6", "start_cell_id_7", "end_cell_id_7",
      "start_cell_id_8", "end_cell_id_8", "start_cell_id_9", "end_cell_id_9", "start_cell_id_10", "end_cell_id_10", "delay_in_sec")
      
      //output.write.option("header","true").mode("overwrite").mode(SaveMode.Append).csv("file:///D:/TCS/OUTPUT")
      output.write.option("header","true").mode("overwrite").csv(FinalOutputPath)
  }
  * */
    
}