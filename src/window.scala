import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row
import org.apache.spark.sql.expressions.Window

object window extends App{
   val sparkConf = new SparkConf()
 sparkConf.set("spark.app.name","my first application")
 sparkConf.set("spark.master","local[2]")

  val spark= SparkSession.builder()//returns builder object
  .config(sparkConf)
  .getOrCreate()
    val nameDf=spark.read
  .format("csv")
  .option("header","true")
  .option("path","C:/Users/PepperPie/Desktop/windowdata.csv")  
  .option("InferSchema","true")
  .load()
  
  
  val namedDf = nameDf.toDF("country", "weeknum", "numOfInvoices", "totalQuantity", "invoiceValue")

 val window= Window.partitionBy("country").orderBy("weeknum")
.rowsBetween(Window.unboundedPreceding,Window.currentRow)//windows size

val mydf=namedDf.withColumn("RunningTotal",sum("invoiceValue").over(window))
mydf.show()
  
  
  
  
  
  
  
  
  
  
  
  spark.stop()
}