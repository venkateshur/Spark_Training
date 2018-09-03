package org.training.spark.programs

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object SampleWordCount {
  
  def main (args:Array[String]) {
    
    val spark = SparkSession
                .builder()
                .appName("sample Workd count")
                .master("local[*]")
                .getOrCreate()
    
    import spark.implicits._
     
    val testData = spark.sparkContext.parallelize((Seq(("A wonderful king is Hadoop"),
                                                    ("The elephant plays well with Sqoop"),
                                                    ("But what helps him to thrive"),
                                                    ("Are Impala, and Hive"),
                                                    ("And HDFS in the group"))))
    val createWordsMap = testData.flatMap(_.split(" ")).map(x=>(x,1))
    val wordCount = createWordsMap.reduceByKey((x,y) => x + y)
    wordCount.collect().foreach(println)
  }
  
}