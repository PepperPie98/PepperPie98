import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

//import org.apache.hadoop.hive.ql.exec.spark.session.SparkSession


object spark_sql_workshop1 extends App{
  val sparkConf = new SparkConf()
 sparkConf.set("spark.app.name","my first application")
 sparkConf.set("spark.master","local[2]")
val spark= SparkSession.builder()//returns builder object
  .config(sparkConf)
  .getOrCreate()
  
    val var1=spark.read
  .format("csv")
  .option("header",true)
  .option("inferschema", true)
  .option("path","C:/Aditya/library/Big data Engineer/Week12/orders.csv")
  .load()
  
 var1.write
.saveAsTable("orders")
spark.stop()
  
  
}