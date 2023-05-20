import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.Row
object Group_agg extends App{
    Logger.getLogger("org").setLevel(Level.ERROR)
  val sparkConf = new SparkConf()
 sparkConf.set("spark.app.name","my first application")
 sparkConf.set("spark.master","local[2]")
  val spark= SparkSession.builder()//returns builder object
  .config(sparkConf)
  .getOrCreate()
  

    val InvoiceDf=spark.read
  .format("csv")
  .option("header","true")
  .option("path","C:/Users/PepperPie/Desktop/order_data.csv")  
  .option("InferSchema","true")
  .load()
  
  //col obj
 val out= InvoiceDf.groupBy("Country","InvoiceNo")
 .agg(sum("Quantity").as("TOtal Qty"),
 sum(expr("UnitPrice * Quantity")).as("Invoice value")
 )
 //col string
 val put2=InvoiceDf.groupBy("country","InvoiceNo")
 .agg(expr("sum(quantity) as TotalQty"),
     expr("sum (Quantity * UnitPrice) as InvoiceValue"))
 
 
  //sql
 InvoiceDf.createOrReplaceTempView("tables")
 spark.sql("""select country, InvoiceNo , sum(Quantity) as TotalQty, sum(UnitPrice * Quantity) as InvoiceValue From tables
   group by country, InvoiceNo"""  ).show()
  
 
  put2.show
  
  spark.stop()
}