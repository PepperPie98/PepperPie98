import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger


object shortform extends App{
  
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]","something")
  val input=sc.textFile("C:/Users/PepperPie/Desktop/dataset.txt").flatMap(x=>x.split(" ")).map(x=>x.toLowerCase()).map(x=>(x,1)).reduceByKey((x,y) => (x+y))
  val input2=input.sortBy(x=>x._2)
  
  val input3= input2.collect
  for (result<-input3){
    var words=result._1
    var value=result._2
    println(s"$words $value")
  }
  
  
//  scala.io.StdIn.readLine()

}