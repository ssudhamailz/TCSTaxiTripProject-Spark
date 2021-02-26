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

class Common {

  val OuputAuditPath = "file:///D:/TCS/Audit"
  val InputSourceDataPath = "file:///D:/TCS/sorted_data.csv"
  val FormattedOutputPath = "file:///D:/TCS/FormattedOutput"
  val FinalOutputPath = "file:///D:/TCS/Output"
  val IsLiveStreaming = "N" //Set to "Y" if the program is running against live streaming data

  // 500 m X 500 m value of longitude and latitude per cell are as follows
  //Converting Meter to Decimal Degree
  //Length in meters of 1° of latitude = always 111.32 km
  //Length in meters of 1° of longitude = 40075 km * cos( latitude ) / 360
 
  val SquareMeter = 500
  val MAPSIZE = 300
  val CenterLatitude = 41.474937
  val CenterLongitude = -74.913585
  val LATITUDE = (SquareMeter / (111.32 * 1000)) //0.004491556
  val LONGITUDE = 0.005681556 //Using online converter "https://www.fcc.gov/media/radio/distance-and-azimuths" i derive the below longitude
  

  def writenull(diff: Int, counter: Int): String =
    {
      var nullrecords = ""

      if (diff <= counter && diff != 0) {
        var r = "NULL," * diff * 2

        nullrecords = r.substring(0, r.length() - 1)

      }
      return nullrecords;

    }

}

