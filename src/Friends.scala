import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger

object Friends extends App{
     Logger.getLogger("org").setLevel(Level.ERROR)  

    def parseLine(line: String)={
  val fields= line.split("::")
  val age=fields(2).toInt
  val fds=fields(3).toInt
  (age,fds)  }
  
  val sc= new SparkContext("local[*]","something")
  val input1=sc.textFile("C:/Users/PepperPie/Desktop/friendsd.csv")
  val input2 = input1.map(parseLine)
  val input3= input2.map(x=>(x._1,(x._2,1)))
  val input4= input3.reduceByKey((x,y) => (x._1+ y._1,x._2+ y._2) )  
 val input5= input4.map(x => (x._1, x._2._1 / x._2._2 ))
 val input6= input5.sortBy(x=>x._1)
  val output=input6.collect()
   output.foreach(println)  
  
  
}