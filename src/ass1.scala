import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger


object ass1 extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]","something")
  val input1=sc.textFile("C:/Users/PepperPie/Desktop/ass1.dataset1")
//  input1.collect.foreach(println)
  val rdd2 = input1.map(line => {
 val fields = line.split(",")
 if (fields(1).toInt > 18)
 (fields(0),fields(1),fields(2),"Y")
 else
 (fields(0),fields(1),fields(2),"N") 
 })
 
 rdd2.collect().foreach(println)
}
/*jlkfjasl f*/