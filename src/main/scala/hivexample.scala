import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.log4j._
import org.apache.spark.storage.StorageLevel
object hivexample{
  def main(args: Array[String]) {

    Logger.getLogger("org").setLevel(Level.ERROR)
    val conf = new SparkConf()
    val spark = SparkSession.builder.appName("Spark-Hive Example").config(conf).enableHiveSupport().getOrCreate()
    import spark.implicits._
    val hiveInput = spark.sql("select * from raviz.orc_taxi").persist(StorageLevel.MEMORY_AND_DISK)

    /* Printing out few results for few columns */
    hiveInput.select("trip_distance","fare_amount","pickup_datetime","dropoff_datetime").show(5)
 /* Checking whether FareAmt + TollAmt + TipAmt without tax application is equivalent to TotalAmt*/
    hiveInput.select(($"fare_amount"+$"tolls_amount"+$"tip_amount" as "expected_amount"),$"total_amount").show(5)
/* Grouping by payment_type column to check which mode of payment is the Max*/
    hiveInput.groupBy("payment_type").agg(count("payment_type")).select("payment_type").show(5)


  }


}