package org.edmond.parseDNSRecords

import org.edmond.DLDistance._
import org.edmond.util._
import org.edmond.Partitioner._
import org.edmond.Dictionary._

import org.apache.log4j.Logger
import org.apache.log4j.Level

import spark.SparkContext
import spark.SparkContext._
import spark.PairRDDFunctions
import spark.OrderedRDDFunctions

import util.control.Breaks._
import scala.io.Source
import scala.io._
import java.io.File
import java.io.FileWriter
import java.io.BufferedWriter
import java.io.IOException

object domainRecords extends Serializable {
	def getDomainRecords(sc: SparkContext, inFile: String, outDir: String, dict: List[List[String]]) = {
		/*
		 *	get all records about domain from input files and save them in outFile. "about" means
		 *	every condidate domain queried by users and have edit distane less than 3 with domain
		 */
		println("inFile: "+inFile)
		println("outDir: "+outDir)

		val threshold = 2
		val dnsRecords = sc.textFile(inFile, 20).map(x => {
			new ParseDNS().convert(x)
		}).filter(r => r._1 != 0 && r._2 != 0 && r._5 != "-")
		
		val listRecords = dnsRecords.map(r => {
			//println("Domain: "+r._5)
			val index = new parseUtils().lookUpInDict(r._5, dict, 0, dict.length-1, threshold)
			if(index > -1){
				//println("HIT!!! "+dict.apply(index).first)
				val domain = dict.apply(index)
				(index, r)
			}
			else{
				(-1, r)
			}
		}).filter(r => r._1 != -1)

		val pairRecords = listRecords.flatMap( line => {
			val res = new scala.collection.mutable.ArrayBuffer[(Int, (Int, Int, String, String, String, Char, List[String], List[Float]))]()
			val domainList = dict.apply(line._1)
			val target = line._2._5
			for(domain <- domainList){
				val dist = new parseUtils().getDistance(target, domain)
				if(dist <= threshold){
					val index = new Dictionary().binary_sesearch(domain, dict, 0, dict.length-1)
					res += ((index, line._2))
				}
			}
			res.toList
		}).cache

		val partitioner = new HashPartitioner(dict.length.asInstanceOf[Int]+1)
		var func = new PairRDDFunctions(pairRecords)
		val data_partitions = func.partitionBy(partitioner)

		data_partitions.foreachPartition(r => {
			if(r.nonEmpty){
				val tmp = r.take(1).toArray
				val index = tmp.apply(0)._1
				//println("index: "+index)
				val filename = dict.apply(index).first
				println("Writing to file: "+filename)
				val file = new File(outDir + filename)
				//if file doesn't exists, then create it
				if(!file.exists()){
					file.createNewFile()
				}
				val fileWriter = new FileWriter(file.getAbsoluteFile(), true)
				val bufferwriter = new BufferedWriter(fileWriter)
				val first_line = new ParseDNS()antiConvert(tmp.apply(0)._2)
				bufferwriter.write(first_line+"\n")	
				r.map(record => new ParseDNS().antiConvert(record._2)).foreach(r => bufferwriter.write(r + "\n"))
				bufferwriter.close
			}
		})
	}

	def main(args: Array[String]): Unit = {
		println("This is a script-started job, getDomainRecords")

		if(args.length < 2){
			println("usage: ./sbt run inFilePath outFileDir")
			exit (0)
		}
		System.setProperty("spark.default.parallelism", "1000")
		Logger.getLogger("spark").setLevel(Level.INFO)

		val sparkHome = "/Users/edmond/spark-0.7.3"
		val jarFile = "target/scala-2.9.3/dnsudf_spark_2.9.3-0.0.jar"
		val master = "local[20]"
		val sc = new SparkContext(master, "dnsudf_spark", sparkHome, Seq(jarFile))

		val outPath = "./res/"
		val dataPath = "/raid/pdns_bro/"
		val webListFile = "./weblist/top1000.dict"

		val webList = new Dictionary().readFromFile(webListFile)

		//val sortFunc = new OrderedRDDFunctions(webList.map(r => (r,1)))
		//val sortedList = sortFunc.sortByKey()
		//val arrWeb = sortedList.map(r => r._1).toArray

		getDomainRecords(sc, args.apply(0), outPath+args.apply(1), webList)
	}
	
}