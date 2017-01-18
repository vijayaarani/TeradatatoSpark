package com.altimetrik.sparksql
import org.apache.spark.SparkConf

import org.apache.spark.sql.SaveMode
import org.apache.spark.SparkContext
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql
import scala.collection.immutable.StringOps
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.Row
import org.apache.spark.sql.types.{StructType,StructField,StringType};
import org.apache.spark.sql.DataFrameWriter
import org.apache.spark.sql.types.DataTypes
import com.databricks.spark.csv.util.{CompressionCodecs, ParserLibs, TextFile, TypeCast}
import org.apache.log4j
import java.util.Properties
import java.lang.System

object UpdateSpark {
   def main (args:Array[String])
  {
     // hadoop-common-bin-winutils configuration
     System.setProperty("hadoop.home.dir","C:\\hadooponwindows-master\\bin\\winutils.exe")
    
    //spark configuration details and application name
      
     val conf = new SparkConf().setAppName("Update to SparkSQL").setMaster("local[*]")
    
    // spark configuration passing to sparkcontext
    
    val sc = new SparkContext(conf)
    //  val in = args(0)
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
        
       val salesDF= sqlContext.createDataFrame(rowRDD, schema)
       
       // dataframe resulted data 
          salesDF.show()
       // val update = salesDF.map(row.getString(1),row.getString(2))
           
        // salesDataFrame convert to temp table 
       
      salesDF.registerTempTable("customer")
     // val sampledf = sqlContext.sql("select * from Test")
      val insert  = sqlContext.sql("select * from customer")
      
      
       // store final results to hdfs in json format
        insert.write.format("json").save("Spark_results")
      // insert.write.format("json").mode("Append").save("json_out")
        
    // departmnet data details      
    val load = sc.textFile("dept.dat")
    //schema definition 
               
    val metadata = "deptid dname dno"  
    // data to schema mapping 
    val meta = StructType(metadata.split(" ").map(fieldName =>StructField(fieldName,StringType,true)))
    
    val mapping = load.map(_.split(",")).map(p => Row(p(0),p(1),p(2)))
   // convert dataframe to temparary table
        
       val deptDF= sqlContext.createDataFrame(mapping,meta )
       
       // dataframe resulted data 
          salesDF.show()
       // val update = salesDF.map(row.getString(1),row.getString(2))
           
        // salesDataFrame convert to temp table 
       
      salesDF.registerTempTable("dept")
                   
    // run sql queries into customer table
                      
       // execute multiple queries
     
        // sqlContext.sql("select * from customer order by loc desc").collect().foreach(println) 
                                             
       //count.collect().foreach(println)
}
}