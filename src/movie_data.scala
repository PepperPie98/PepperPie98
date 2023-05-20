import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger


object movie_data extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]","something")
  val input1= sc.textFile("C:/Users/PepperPie/Desktop/movied.data")
  val input2=input1.map(x=> x.split("\t")(2))
  val input3=input2.map(x=>(x,1))
  val input4=input3.reduceByKey((x,y)=>(x+y))
  
  val out=input4.collect()
  //out.foreach(println)
  for(output<-out){
    println(output)
  }
}