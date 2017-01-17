package com.altimetrik.bteq.parsing
import scala.io.Source

object Inserstatement {
  
   def main (args:Array[String])
  {
    
    // reading data from file and using getlines method to return line by line
    val data = Source.fromFile("bteqscripts").getLines()
    // apply filter fucntion and select only insert statement
    val sel = data.filter(_ startsWith("insert"))
    // return only insert statement
    sel.foreach(println)
      
}}