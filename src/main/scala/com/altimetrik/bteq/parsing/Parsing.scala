package com.altimetrik.bteq.parsing
import scala.io.Source

object Parsing {
  def main (args:Array[String])
  {
       // reading queries from Bteqscrits
   
       // input file path 
    //  val  data = args(0)
     // reading data from line by line scala
      
      
     // val raw = Source.fromFile(data).getLines().filter(_ startsWith("sel"))
      val raw = Source.fromFile("bteqscripts").getLines()
      val sel = raw.filter(_ startsWith(".import"))
     
     // using map and replace some bteq commands to sparksq
     // val rep = raw.map(_.replace("", "select"))
     // printing the final results
     sel.foreach(println)
  }   
}