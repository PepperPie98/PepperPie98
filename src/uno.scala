import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger



object uno extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
   val sc = new SparkContext("local[*]","something")
   val input=sc.textFile("C:/Users/PepperPie/Desktop/dataset.txt")
 val input1=input.flatMap(x=>(x.split(" ")))
 val input2=input1.map(x=>(x,1))
 val input3=input2.reduceByKey((x,y)=>(x+y))
 input3.collect.foreach(println)
 scala.io.StdIn.readLine()

}