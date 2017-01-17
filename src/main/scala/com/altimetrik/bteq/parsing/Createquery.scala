package com.altimetrik.bteq.parsing
import scala.io.Source
import scala.collection.Iterator

 object Createquery {
   
   def main (args:Array[String])
  {
       // reading queries from Bteqscrits
   
      // reading data from line by line scala and selecting only create statement
       
     val raw = Source.fromFile("bteqscripts").getLines()
      // using filter we are selecting create statement only 
     val create = raw.filter(_ startsWith("create"))
     
      // create statement     
      create.foreach(println)
     
      // printing the final results
        }   
}