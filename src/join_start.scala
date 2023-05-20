import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object join_start extends App{
 val sparkConf = new SparkConf()
 sparkConf.set("spark.app.name","my first application")
 sparkConf.set("spark.master","local[2]")
  val spark= SparkSession.builder()//returns builder object
  .config(sparkConf)
  .getOrCreate()
  
  
  val orderFile=spark.read
  .option("path","C:/Users/PepperPie/Desktop/orders.csv")
  .format("csv")
  .option("header","true")
  .load()
  val CustomerFile=spark.read
  .option("path","C:/Users/PepperPie/Desktop/customers.csv")
  .format("csv")
   .option("header","true")
  .load()
 // orderFile.show()
  val joinCol = orderFile.col("order_customer_id")=== CustomerFile.col("customer_id")//Join column
  val joinType = "outer"//type of join
  val finalJoin=orderFile.join(CustomerFile,joinCol,joinType).sort("order_customer_id")
  
  
  finalJoin.show()
//  scala.io.StdIn.readLine()

  spark.stop()
}