import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger


object top_ten_Customers extends App{
 Logger.getLogger("org").setLevel(Level.ERROR)
 val sc=new SparkContext("local[*]","customer-orders")
 val input=sc.textFile("C:/Users/PepperPie/Desktop/customerorders.csv")
 val input2=input.map(x=>(x.split(",")(0),(x.split(",")(2).toFloat)))
// val input3=input.map(x=>(x._0,x._2))
 val input3=input2.reduceByKey((x,y)=>(x+y))
 val srt=input3.sortByKey()

 val input4=srt.collect()
// for (result <- input4){
//   println(result)
// }
 input4.foreach(println)
}
