package com.naresh.org
import com.datastax.spark.connector._ 
import com.datastax.driver.core._
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.apache.spark.SparkConf
object StoringIntoCassandra {
  def main(args: Array[String]): Unit =
  {
  val conf = new SparkConf(true)
  conf.set("spark.cassandra.connection.host","localhost")
  val sc = new SparkContext("local[2]","test",conf)
    
    val personrdd = sc.textFile("file:///home/hadoop/Relation")
    case class person(id:Int,name:String,place:String,city:String)
    val persondf = personrdd.map(_.split(",")).map(row=>person(row(0).toInt,row(1).toString,row(2).toString,row(3).toString))
    persondf.saveToCassandra("stackoverflow","person")
s   persondf.saveAsCassandraTable("stackoverflow","person1") // use if table doesnt exist
    
  }

}