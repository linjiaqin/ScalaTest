package com.ljq

import org.apache.spark.{SparkConf, SparkContext}
import org.json4s.DefaultFormats
import org.json4s._
import org.json4s.jackson.JsonMethods._

case class Person(name: String, age: Int)

object JsonTest {
  def main(args:Array[String])={
    var conf = new SparkConf().setAppName("JsonTest").setMaster("local")
    var jarPath = "/home/linjiaqin/IdeaProjects/ScalaTest/out/artifacts/ScalaTest_jar/ScalaTest.jar"

    var sc = new SparkContext(conf)
    sc.addJar(jarPath)

    var hdfs = "hdfs://localhost:9000"
    var input = hdfs + "/linjiaqin/3_data/testjson.json"

    var jsontext = sc.textFile(input)

    implicit val formats = DefaultFormats

    var person = jsontext.collect().map(x => parse(x).extract[Person] )
    person.foreach(println)
  }
}
