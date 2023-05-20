import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext


object campaign extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
 val sc=new SparkContext("local[*]","something")
 val input1=sc.textFile("C:/Users/PepperPie/Desktop/bigdatacampaign.csv")
 val input2=input1.map(x=> (x.split(",")(10).toFloat,x.split(",")(0)))
 val input3=input2.flatMapValues(x=>x.split(" "))
 val input4=input3.map(x=> (x._2.toLowerCase,x._1))
 val input5=input4.reduceByKey((x,y)=> x+y )
 
 
 val input6= input5.sortBy(x=> x._2,false).collect
input6.foreach(println)
input5.saveAsTextFile("C:/Users/PepperPie/Desktop/badaboom")


}