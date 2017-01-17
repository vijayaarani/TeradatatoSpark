package com.altimetrik.sparksql

import java.util.Properties
import java.sql.DriverManager
import java.sql.Statement

object ScalajdbctoTeradata {
  
 def main(args:Array[String])
 {
 
  val url = "jdbc:teradata://192.168.118.129/database=spark,TMODE=ANSI,charset='UTF8'"
  val properties = new java.util.Properties()
  val driver = "com.teradata.jdbc.TeraDriver"
  properties.setProperty("driver","driver")
  properties.setProperty("username","dbc")
  properties.setProperty("password","dbc")
  val conn = DriverManager.getConnection(url,properties)
  val query:String = "update test set name = 'suman' where id = 104;update test set name = 'AFB' where id =100 "
  Class.forName(driver).newInstance()

  var stmt = conn.createStatement()
  val ps = conn.prepareStatement(query)

 val rs = ps.execute()
 println(rs)

 }
}
