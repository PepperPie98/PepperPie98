import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger


object practical_two extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]","something")
  val input=sc.textFile("C:/Users/PepperPie/Desktop/dataset.txt")
  val input2=input.flatMap(x=>x.split(" "))
  val words=input2.map(x=>x.toLowerCase())
  //val input3=input2.map(x=>(x,1))
  val input3=words.map(x=>(x,1))
  val input4=input3.reduceByKey((x,y) => (x+y))
  val input5=input4.map(x=> (x._2,x._1))
  val input6=input5.sortByKey(false)
  val input7=input6.map(x=> (x._2,x._1))
  val results = input7.collect
  for (finals <- results){
   val keys=finals._1
   val values=finals._2
   println(s"$keys  $values")
  }
  
  
//  scala.io.StdIn.readLine()

}