package com.altimetrik.sparksql

import scala.io.Source
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql
import org.apache.spark.sql.hive.HiveContext

object HiveSparkSQL {
  def main(args:Array[String])
  {
    
    // spark configuration details and master details
   val conf = new SparkConf().setAppName("hive-Sparksql").setMaster("local")
   // passing sparkconf to sparkcontext
   val sc = new SparkContext(conf)
   // creating hivecontext to hive
   val  hiveContext = new HiveContext(sc)
   // reading hive metastore and extract hive query data to spark and store into dataframe 
   // val query = "  "
      val df = hiveContext.sql("select * from customers")
      // using dataframe show command we can see the query data
                     df.show()
       // apply count function count the no of customer              
              println("Total Records " + df.count())
             
            df.write.format("orc").save("hive_results")          
   
            }}
