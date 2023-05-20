import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object Simpled_Agg extends App{
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
 val out= InvoiceDf.select(count("*").as("no of rows"),
      sum("Quantity").as ("TotalQty"),
      avg("UnitPrice").as("average"),	
     countDistinct("InvoiceNo").as("distinct invoice") )
 //col string
 val out2=InvoiceDf.selectExpr("count(*)as no_of_rows",
     "sum(Quantity) as TotalQty",
     "avg(UnitPrice) as average",
     "count(Distinct(InvoiceNo)) as distinct_invoice")    
 
 //sql
 InvoiceDf.createOrReplaceTempView("tables")
 spark.sql("""select count(*)as no_of_rows, sum(Quantity) as TotalQty,
     avg(UnitPrice) as average,
     count(Distinct(InvoiceNo)) as distinct_invoice   FROM tables"""  ).show()
     
 out2.show()
  spark.stop()
}