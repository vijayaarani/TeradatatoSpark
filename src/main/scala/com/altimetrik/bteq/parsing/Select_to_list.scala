package com.altimetrik.bteq.parsing
import scala.io.Source
import scala.collection.JavaConversions._
import scala.collection.immutable.StringOps

object Select_to_list {
  
  def main(args:Array[String])
  {
  // reading data from file
  val invMapData = Source.fromFile("bteqscripts")
  // using getline method to get the file data 
  val invLines = invMapData.getLines
  // apply filter condition and select only select statements
  val sel = invLines.filter(_ startsWith("select"))
  // using replace function replace sel string to select 
 // val rep = sel.map(_.replace("sel","select"))
  //Using mkString method to convert all select statements to  
  val list = sel.mkString
  val spliting = list.split(";")
  spliting.foreach(println)
       
}}