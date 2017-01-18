package com.altimetrik.sparksql

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql
import scala.collection.immutable.StringOps
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType,StructField,StringType};
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.types.DataTypes
import com.databricks.spark.csv.util.{CompressionCodecs, ParserLibs, TextFile, TypeCast}
import org.apache.log4j
import java.util.Properties
import java.lang.System

object SparksqlConverter {
  
  def main (args:Array[String])
  {
     // hadoop-common-bin-winutils configuration
     System.setProperty("hadoop.home.dir","C:\\hadooponwindows-master\\bin\\winutils.exe")
    
    //spark configuration details and application name
      
     val conf = new SparkConf().setAppName("Bteq to SparkSQL").setMaster("local[*]")
    
    // spark configuration passing to sparkcontext
    
    val sc = new SparkContext(conf)
     // val in = args(0)
     // val out = args(1)
    
   // import sqlcontext and create 
    
    val sqlContext = new SQLContext(sc)
    
    // data loding to spark 
    
    val data = sc.textFile("test.txt")
    //schema definition 
               
    val schemaString = "custid custname loc sal gender"  
    // data to schema mapping 
    val schema = StructType(schemaString.split(" ").map(fieldName =>StructField(fieldName,StringType,true)))
    
    val rowRDD = data.map(_.split(",")).map(p => Row(p(0),p(1),p(2),p(3),p(4)))
   // convert dataframe to temparary table
        
       val salesDataFrame = sqlContext.createDataFrame(rowRDD, schema)
       
       // salesDataFrame convert to temp table 
       
       salesDataFrame.registerTempTable("sales")
             
    // run sql queries into sales table
         
    //  val salesdata = sqlContext.sql("select * from sales").collect().foreach(println)
   // val sparksql = sqlContext.sql("bteqquery").collect().foreach(println)
                   
       // execute multiple queries
     
         sqlContext.sql("select * from sales order by loc desc").collect().foreach(println) 
         sqlContext.sql("select custid,custname from sales").collect().foreach(println)

         
         
         // sqlContext.sql("select * from deepthi").collect().foreach(println)                              
       //count.collect().foreach(println)
      
  } 
}