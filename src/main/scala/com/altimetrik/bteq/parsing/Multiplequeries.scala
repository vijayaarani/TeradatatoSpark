package com.altimetrik.bteq.parsing
import scala.io.Source

object Multiplequeries {
  
  def main (args:Array[String])
   {
    // reading commands  from bteqscrits
    val data = Source.fromFile("bteqscripts").getLines()
      // select specific queries using starts with  
     val lines = data.filter(_ startsWith("sel"))
     val rep = lines.map(_.replace("sel","select"))
     // convert all lines to string 
     val multiple = lines.mkString
     
     // return only select statements
       println(multiple)
}}
       
     

