import org.apache.spark.sql.SparkSession
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.functions._



object multiplecsvs {
  def main(args: Array[String]){


    val conf = new SparkConf().setMaster("local").setAppName("abcd")
    val sc = new SparkContext(conf)
    val spark = SparkSession.builder.config(conf).getOrCreate()
    import spark.implicits._
    /* we can use * symbol as shown below if we need to read all csv files to an RDD*/
    val multiplecsvs = sc.textFile("file:///Users/ravitejacheruvu/Downloads/*.csv")
     print(multiplecsvs.count)
    /* we have to use wholeTextFiles if we want to extract the file name we loaded onto a RDD */
    val mulcsvs = sc.wholeTextFiles("file:///Users/ravitejacheruvu/Downloads/mine") /* mine is a directory */
    val df=mulcsvs.toDF()
      df.select("_1").show(false)/* this would show file:/Users/ravitejacheruvu/Downloads/mine/SalesJan2009.txt */
     df.withColumn("file_name",split(df("_1"),"/")).select($"file_name".getItem(5).as("gotit")).show(false)

  }
}
