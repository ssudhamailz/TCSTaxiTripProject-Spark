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
import java.io.{ File, FileNotFoundException, IOException }
import scala.util.{ Try, Success, Failure }

class DataProcessing {

  def ProcessedData(): DataFrame = {

    val path = new Common()

    val spark: SparkSession = SparkSession.builder()
      .master("local[*]")
      .appName("ProcessData")
      .getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    import spark.sqlContext.implicits._

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

    val rawdata = spark.read.schema(trip_schema).csv(path.InputSourceDataPath)
    //rawdata.printSchema()

    //rawdata.show()

    rawdata.createOrReplaceTempView("vw_trip_rawdata")

    //val gridcell = spark.read.csv("file:///D:/TCS/GridMap.csv")

    //gridcell.createOrReplaceTempView("vw_gridcell")

    /*Data filtering to remove incorrect records*/

    val processeddata = spark.sql("""select * from vw_trip_rawdata where                                 		  
		                                  pickup_datetime <> dropoff_datetime AND 
                                		  trip_distance > 0 AND total_amount > 0 AND 
                                		  pickup_longitude <> dropoff_longitude AND 
                                		  pickup_latitude <> dropoff_latitude AND 
                                		  pickup_longitude <> 0 AND 
                                		  pickup_latitude <> 0 AND 
                                		  dropoff_longitude <> 0 AND 
                                		  dropoff_latitude <> 0""")

    return processeddata
  }

}
