import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext


object movies_prac extends App{
    Logger.getLogger("org").setLevel(Level.ERROR)
   val sc = new SparkContext("local[*]","something")
   val inputFile= sc.textFile("C:/Aditya/library/Big data Engineer/Week11/ratings.dat") 
   val input1=inputFile.map(x=> {
    val columns = x.split("::")
     (columns(1).toInt,columns(2).toFloat)
   })
   val input2=input1.mapValues(x => (x,1.0))
   val input3=input2.reduceByKey((x,y)=> ((x._1+y._1),(x._2+y._2)))
   val input4= input3.filter(x=> (x._2._2 )> 1000)
   val input5=input4.mapValues(x=> (x._1/x._2))
   val input6= input5.filter(x=> (x._2)>4.5)
  
   
   val movieFile= sc.textFile("C:/Aditya/library/Big data Engineer/Week11/movies.dat") 
   val movieInput= movieFile.map(x=> {
    val col= x.split("::")
    (col(0).toInt,col(1))
    })

   val combine=input6.join(movieInput)
   val finale=combine.map(x=> (x._1,x._2._2))
   val topMovie= finale.collect()
   topMovie.foreach(println)
   
   
    scala.io.StdIn.readLine()

}