package com.altimetrik.sparksql
import java.sql.DriverManager
import java.sql.DriverManager
import java.sql.Connection

    object JdbcTeradata{
  
    def main(args:Array[String])
  {
 
     // connect to the database named "teradata" on the localhost
   
    val driver = "com.teradata.jdbc.TeraDriver"
     
    val url = "jdbc:teradata://192.168.118.129/database=spark,TMODE= 'ANSI',charset =UTF8"
    val username = "dbc"
    val password = "dbc"
 
    // there's probably a better way to do this
    var connection:Connection = null

    try {
      // make the connection
      Class.forName(driver)
      connection = DriverManager.getConnection(url, username, password)

      // create the statement, and run the select query
      val statement = connection.createStatement()
      //val query:String = "create view deepu as select * from spark.test"
      val query:String = "update test set name = 'User' where id = 104;update test set name = 'IVE' where id =100 "
      val result = statement.executeQuery(query)
           
        println("success")
      
    } catch {
      case e => e.printStackTrace()
    }
    connection.close()
  }

}
