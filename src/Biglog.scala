import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkConf
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.DateType
import org.apache.spark.sql.functions._
import org.apache.spark.sql.Row



object Biglog extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
    case class Logging(level:String, datetime:String)
    
    def mapper(line:String):Logging={
      val field = line.split(',')
      val logging:Logging=Logging(field(0),field(1))//casting to case class
      return logging
    }

     val sparkConf = new SparkConf()
 sparkConf.set("spark.app.name","my first application")
 sparkConf.set("spark.master","local[2]")

  val spark= SparkSession.builder()//returns builder object
  .config(sparkConf)
  .getOrCreate()

    import spark.implicits._
/*  val myList=List("DEBUG,2015-2-6 16:24:07",
"WARN,2016-7-26 18:54:43",
"INFO,2012-10-18 14:35:19",
"DEBUG,2012-4-26 14:26:50",
"DEBUG,2013-9-28 20:27:13")
val input1=spark.sparkContext.parallelize(myList)
val input2=input1.map(mapper)

 val df1=input2.toDF()

  df1.createOrReplaceTempView("log_table")
/* spark.sql("""select level, collect_list(datetime) from log_table
   group by level""").show(false)
 
 */
  
 /*
  spark.sql("""select level, count(datetime) from log_table
   group by level order by level""").show(false)
 */
 
  /*
   * spark.sql("""select level, date_format(datetime,'MMMM')as Month  from log_table
   """).show(false)*/
  
  
  
val df2= spark.sql("""select level, date_format(datetime,'MMMM')as Month from log_table""")
 //df2.show
   

df2.createOrReplaceTempView("lm_table")
spark.sql("select level, Month,count(1) from lm_table group by level,Month").show
 
//count(1) used for count of each level, month
*/
//file starts here
val start1=  spark.read
  .option("header",true)
  .csv("C:/Users/PepperPie/Desktop/biglog.txt")
 start1.createOrReplaceTempView("my_log_table")
 /*
  val start2= spark.sql("""select level, date_format(datetime,'MMMM') as Month, (date_format(datetime,'M')) as month_num,count(1) as total from my_log_table
    group by level, Month,month_num order by month_num""")
 
 */
  
 /*val start2= spark.sql("""select level, date_format(datetime,'MMMM') as Month, cast(first(date_format(datetime,'M')) as int) as month_num,count(1) as total from my_log_table
    group by level, Month order by month_num""")
 
 */
 
 val start2= spark.sql("""select level, date_format(datetime,'MMMM') as Month, cast(first(date_format(datetime,'M')) as int) as month_num,count(1) as total from my_log_table
    group by level, Month order by month_num,level""")
    
 val result=start2.drop("month_num")
 result.show(60)
 
 
 

 
 
   spark.stop() 
}