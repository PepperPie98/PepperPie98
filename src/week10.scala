import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger


object week10 extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]","something")
  val courseFileinput=sc.textFile("C:/Users/PepperPie/Desktop/assignment dataset/chapters.csv")
  val mappedCourseFile=courseFileinput.map(x=>(x.split(",")(1),1))
  val countOfCourse=mappedCourseFile.reduceByKey((x,y)=> x+y)
  
  val output=countOfCourse.collect
  output.foreach(println)
}