import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.log4j.Level
import org.apache.log4j.Logger
import breeze.linalg.where


object orders_DS extends App{
 Logger.getLogger("org").setLevel(Level.ERROR)

 val sparkConf = new SparkConf()
 sparkConf.set("spark.app.name","my first application")
 sparkConf.set("spark.master","local[2]")
 
 val myreg="""^(\S+) (\S+)\t(\S+)\,(\S+)""".r
  case class Orders(order_id:Int,customer_id:Int,order_Status:String)

 def parse(takeInput:String)={
       takeInput match{
         case myreg(order_id,date,customer_id,order_Status)=>
           Orders(order_id.toInt,customer_id.toInt,order_Status)
       }
     }

   val spark= SparkSession.builder()//returns builder object
   .config(sparkConf)
   .getOrCreate()
  import spark.implicits._
   val input1= spark.sparkContext.textFile("C:/Users/PepperPie/Desktop/order_test.txt")
   val takeInput=input1.map(parse).toDS.cache()
   takeInput.printSchema()
 // takeInput.select("order_id").show
   takeInput.groupBy("order_Status").count().show
  
  // takeInput.createOrReplaceTempView("orders")
 //  val res= spark.sql("select customer_id,order_Status from orders  where order_Status is 'PAID'").show
   

  
   spark.stop()
}