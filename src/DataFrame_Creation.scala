import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession

object DataFrame_Creation extends App{
 val sparkConf = new SparkConf()
 sparkConf.set("spark.app.name","my first application")
 sparkConf.set("spark.master","local[2]")
 
 val spark= SparkSession.builder()  //returns builder object
  .config(sparkConf)
  .getOrCreate()
  
  
  val orders=spark.read.option("header",true)
  .option("inferSchema",true)
  .csv("C:/Aditya./library/Big data Engineer/Week11/orders.csv")
  orders.show()
  orders.printSchema()
  
  
  spark.stop()
  

}