import org.apache.log4j.Level
import org.apache.log4j.Logger
import org.apache.spark.SparkContext


object Final_Partweek10 extends App{
    Logger.getLogger("org").setLevel(Level.ERROR)
    val sc = new SparkContext("local[*]","something")
  
  //exc1
  val courseFileinput=sc.textFile("C:/Users/PepperPie/Desktop/assignment dataset/chapters.csv")
  val mappedCourseFile=courseFileinput.map(x=>(x.split(",")(1).toInt,1))
  val ChaptercountOfCourse=mappedCourseFile.reduceByKey((x,y)=> x+y) 
  
  //exc2
  
  val viewFileinput=sc.textFile("C:/Users/PepperPie/Desktop/assignment dataset/views*.csv")
  val mappedView=viewFileinput.map(x=>(x.split(",")(0).toInt,x.split(",")(1).toInt)).distinct()
  
  //join normal Course1 and  flipped distinct view
  val input1= courseFileinput.map(x=>(x.split(",")(0).toInt,x.split(",")(1).toInt))
  val input2= mappedView.map(x=>(x._2,x._1))
  val joinedoutput1= input2.join(input1)
  val dropChapter=joinedoutput1.map(x=> ((x._2._1,x._2._2),1))

  val addcount=dropChapter.reduceByKey((x,y)=>(x+y))
 

//dropping userid
  val viewPerCourse=addcount.map(x=> (x._1._2,x._2))
  val joinedoutput2=viewPerCourse.join(ChaptercountOfCourse)
  val divide_view=joinedoutput2.mapValues(x=> x._1.toDouble/x._2)
//  val percent_view=divide_view.map(x=> f"$x%01.5f".toDouble)
  
  
  //score calculation block
  val score_Calc=divide_view.mapValues(x=>
   if(x>0.9) 10
   else if(x>0.5 && x<0.9) 4
   else if(x>0.25 && x<0.5) 2
   else 0
  )
  
val scoreCard=score_Calc.reduceByKey((x,y) =>x+y)

  
  
  //get title file
  val titleFileInput=sc.textFile("C:/Users/PepperPie/Desktop/assignment dataset/titles.csv").map(x=> (x.split(",")(0).toInt,x.split(",")(1)))
  val joinedOutput3=scoreCard.join(titleFileInput).map(x=> (x._2._1,x._2._2)).sortByKey(false)
  val finallyGotOutput=joinedOutput3.collect
 finallyGotOutput.foreach(println)
  
}
