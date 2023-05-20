import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger


object week10_exc2 extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)

  
  
  
  val sc = new SparkContext("local[*]","something")
  val viewFileinput=sc.textFile("C:/Users/PepperPie/Desktop/assignment dataset/views1.csv")
  val mappedView=viewFileinput.map(x=>(x.split(",")(0).toInt,x.split(",")(1).toInt)).distinct()

  val output=mappedView.collect
   output.foreach(println)
 
  
  
  
}