import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext
import breeze.linalg.min



object tempdata extends App{
  Logger.getLogger("org").setLevel(Level.ERROR)
  val sc = new SparkContext("local[*]","something")
  def parsedline (field:String)={
  val fields=field.split(",")
  val id=fields(0)
  val temp_var=fields(2)
  val tem=fields(3).toFloat
  (id,temp_var,tem)
}
  val input1=sc.textFile("C:/Users/PepperPie/Desktop/tempdata.csv")
  val temp=input1.map(parsedline)
  val temp1=temp.filter(x=> (x._2 == "TMIN"))
  val temp2=temp1.map(x=> (x._1,x._3.toFloat))
  val temp3=temp2.reduceByKey((x,y)=> min(x,y))
  val output1=temp3.collect()
//  output1.foreach(println)
   for(result<-output1){
     val station=result._1
     val num=result._2
     val finalnum=f"$num%.2f F"
     println(s"$station lowest temp: $finalnum")
   }
}