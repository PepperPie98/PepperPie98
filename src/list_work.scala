import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger


object list_work extends App {
  Logger.getLogger("org").setLevel(Level.ERROR)
 val sc = new SparkContext("local[*]","something")
 val myList=List("WARN : Tuesday 4 April 0405",
"ERROR : Tuesday 4 April 0409",
"ERROR : Tuesday 4 April 0409",
"ERROR : Tuesday 4 April 0409",
"ERROR : Tuesday 4 April 0409")

val input1= sc.parallelize(myList)
val input2=input1.map(x=>    {
 val columns=x.split(":")
 val loglevel=columns(0)
 (loglevel,1)
})
val outpit=input2.reduceByKey((x,y)=> x+y)
outpit.foreach(println)

}