import org.apache.spark.streaming.StreamingContext
import org.apache.spark.streaming.Seconds
import org.apache.spark.SparkContext
import org.apache.log4j.Level
import org.apache.log4j.Logger


object Stream1 extends App{
    Logger.getLogger("org").setLevel(Level.ERROR)

    val sc = new SparkContext("local[*]","something")
    val ssc = new StreamingContext(sc, Seconds(2))
//lines is a dstream
val lines = ssc.socketTextStream("localhost",9998)
//words is a transformed dstream
val words = lines.flatMap(x => x.split(" "))
val pairs = words.map(x => (x,1))
val wordCounts = pairs. reduceByKey((x,y) => x+y)
wordCounts.print()
ssc.start() 
ssc.awaitTermination()
}
