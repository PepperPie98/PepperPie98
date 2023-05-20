import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession


object week12_trail extends App{
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
  .option("path","C:/Aditya/library/Big data Engineer/Week12/customers.csv")
  .load()
  
  
  //var1.show()
  var1.write
  .option("path","C:/Users/PepperPie/Desktop/manwrite")
  .save()
  spark.stop()
}