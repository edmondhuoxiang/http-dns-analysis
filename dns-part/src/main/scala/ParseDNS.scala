package org.edmond.parseDNSRecords

import spark.SparkContext
import spark.SparkContext._
import spark.util.Vector

import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.util.control.Breaks._
import scala.util.Random
import scala.io.Source
import scala.io._
import java.io.File

/*
	Parse data from raw data file of DNS recode. The format of each line is as following:
	#fields ts id.orig_h id.resp_h query rcode answers TTLs
	1. timestamp of dns request
	2. requester IP
	3. responder IP
	4. query name
	5. response code
	6. list of answers, use ',' to seperate each answer
	7. list of TTLs associated with each answer, use ',' to seperate each TTL

	use `new ParseDNS().convert(line) to convert each line from string to data structure in memory
	use `new ParseDNS().antiConvert(record) to convert each data structure to string line. 

	if a line started with '#', it will be considered as comment, and will return a record with 0 as ts, use filter
	can filter out those lines. 
*/

class DNSInfo(
	//0: ts
	val ts: String,
	//1: src_ip
	val src_ip: String,
	//2: dst_ip
	val dst_ip: String,
	//3: query domain name
	val domain: String,
	//4: response code
	val rcode: Char, 
	//5: Everything else
	val remaining: String)
{
	val parsed_answer : (List[String],List[Float]) = parseAnswer()

	def toTuple(): (Int, Int, String, String, String, Char, List[String], List[Float]) = {
		val arr = ts.split('.')
		val out = (arr(0).toInt,
			arr(1).toInt,
			src_ip,
			dst_ip,
			domain.toLowerCase,
			rcode,
			parsed_answer._1,
			parsed_answer._2)
		return out
	}

	def parseAnswer() : (List[String], List[Float]) = {
		try{
			//Parse the the list of answer and list of TTLs associated with each anwser
			val answers = new scala.collection.mutable.ListBuffer[String]()
			for(str <- remaining.split(' ')){
				if(str != "")
					answers += str
			}
			val ip_lst = new scala.collection.mutable.ListBuffer[String]()
			
			for(ans <- answers.apply(0).split(',')){
				ip_lst += ans
			}
			val ttl_lst = new scala.collection.mutable.ListBuffer[Float]()
			for(ttl <- answers.apply(1).split(',')){
				ttl_lst += ttl.toFloat
			}
			return (ip_lst.toList, ttl_lst.toList)
		} catch {
			case e: Exception => {
				println("failed to parse: " + e)
				return (Nil, Nil)
			}
		}
	}

	override def toString() : String = "ts=%s, src_ip=%s, dst_ip=%s, domain=%s, rcode=%c".format(ts, src_ip, dst_ip, domain, rcode)

}

class ParseDNS(){
	def print() = {
		println("This is from ParseDNS Class")
	}

	def convert(line: String) : (Int, Int, String, String, String, Char, List[String], List[Float]) = {
	//	println("#####")
	//	println(line)
		if(line.first != '#'){
			val arr = org.apache.commons.lang.StringUtils.split(line," ")
			val parts = scala.collection.mutable.ListBuffer[String]()
			for(part <- arr){
				if(part != "")
					parts += part
			}
		
			val str = new scala.collection.mutable.StringBuilder()
			str ++= parts(5)
			for(i <- 6 until parts.size){
				str ++= " " + parts(i)
			}
			return (new DNSInfo(parts(0), parts(1), parts(2), parts(3), parts(4).head, str.toString)).toTuple
		}
		else
			return (0, 0, "", "", "", '-', Nil, Nil)
	}

	def antiConvert(record: (Int, Int, String, String, String, Char, List[String], List[Float])) : String = {
		val line = new scala.collection.mutable.StringBuilder()
		line ++= record._1.toString+" "+record._2.toString+" "
		line ++= record._3+" "+record._4+" "+record._5+" "+record._6.toString+" "
		for(i <- 0 until record._7.size){
			line ++= record._7.apply(i)
			if(i < record._7.size-1)
				line ++= ","
		}
		line ++= " "
		for(i <- 0 until record._8.size){
			line ++= record._8.apply(i).toString
			if(i < record._8.size-1)
				line ++= ","
		}
		return line.toString
	}
}

/*object test {
	def main(args: Array[String]): Unit = {
		val outPath = "./"
		Logger.getLogger("spark").setLevel(Level.WARN)
		val sparkHome = "/Users/edmond/spark-0.7.3"
		val jarFile = "target/scala-2.9.3/dnsudf_spark_2.9.3-0.0.jar"
		val master = "local[1]"
		val sc = new SparkContext(master, "dnsudf_spark", sparkHome, Seq(jarFile))

		val raw = sc.textFile("/raid/pdns_bro/20130821/1377061310.pcap0000000.log.gz")

		val data = raw.map(x => new ParseDNS().convert(x)).filter(r => r._1 != 0 && r._2 != 0)

		data.foreach(r => {
			val line = new ParseDNS().antiConvert(r)
			println(line)
		})
	}
*/
}