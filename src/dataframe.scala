import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row



object dataframe extends App{
     val sparkConf = new SparkConf()
 sparkConf.set("spark.app.name","my first application")
 sparkConf.set("spark.master","local[2]")

  val spark= SparkSession.builder()//returns builder object
  .config(sparkConf)
  .getOrCreate()
  val myList =List(
  (1,"1-5-2021",1001,"PAID"),
  (2,"02-23-2021",15050,"PENDING"),
  (3,"2021-02-25",2155,"PAID"),
  (4,"2021-02-25",15050,"CONFIRMED"),
  (5,"2022-02-23",2566,"PAID"))
   
  
  //val DF1=spark.createDataFrame(myList) 
  
 //import spark.implicits._
val DF1= spark.createDataFrame(myList).toDF("order_id","date","customer_id","status")
 val DF2= DF1.withColumn("date",unix_timestamp(column("date").cast(DateType)))
  .withColumn("new_id",monotonically_increasing_id)
  .drop("order_id")
  .dropDuplicates("customer_id")
  .sort("status")
  
  
  //DF1.printSchema()
  
  DF2.show
  
  
  spark.stop()
}