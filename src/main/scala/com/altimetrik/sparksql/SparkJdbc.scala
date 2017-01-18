package com.altimetrik.sparksql
import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql
import java.util.Properties
object SparkJdbc {
  
  def main(args:Array[String])
  {
    val conf = new SparkConf().setAppName("jdbc Teradata Driver").setMaster("local")
    val sc = new SparkContext(conf)
    val sqlContext = new SQLContext(sc)
    import sqlContext.implicits._
   val jdbcDF = sqlContext.read.format("jdbc")
  .option("url", "jdbc:teradata://192.168.118.129/database=spark TMODE= ANSI,charset= 'UTF8'")
  .option("driver","com.teradata.jdbc.TeraDriver")
  .option("dbtable", "spark.test")
  .option("user", "dbc")
  .option("password", "dbc")
  .load()
  jdbcDF.show()
  jdbcDF.printSchema()
  jdbcDF.registerTempTable("test")
  jdbcDF.write.format("json").save("teradata_results")
  
}
}