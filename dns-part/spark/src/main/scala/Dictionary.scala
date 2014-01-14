package org.edmond.Dictionary

import org.apache.log4j.Logger
import org.apache.log4j.Level

import spark.SparkContext
import spark.SparkContext._

import scala.io.Source
import scala.io._
import java.io.File
import java.io.FileWriter
import java.io.BufferedWriter

import org.edmond.DLDistance._

class Dictionary(){
	/* 
		The Dictionary class can make a list of domain string like a dictionary. The result of the dictionary 
		is List[List[String]]. The first element of a List[String] is the 'target' domain, and every string 
		following it is the similar domain which has edit distance less than a given threshold with the 'target'.
		And the result is sort by the first element of each List[String]
	*/	
	def getSimilarString(target: String, strList: List[String], threshold: Int): List[String] = {
		// Get all similar strings from a string list when a target string is given. 
		// Compared with target string, the strings in the result list should have edit distance 
		// less than threshold but more than zero
		val similarSet = new scala.collection.mutable.ArrayBuffer[String]()

		for( str <- strList) {
			val dist = new DLDistance().distance(target, str)
			if(dist <= threshold && dist > 0){
				similarSet += str
			}
		}
		return similarSet.toList
	}

	def createDictionary(path: String, threshold: Int): List[List[String]] = {
		// Create the Dictionary structure in memomery when a path is given. 
		// The input file should store all possible domains. 
		println("Creating Dictionary...")
		val dictionary = new scala.collection.mutable.ArrayBuffer[List[String]]()
		val similarList = new scala.collection.mutable.ArrayBuffer[String]()
		try { 
			val strList = scala.io.Source.fromFile(path).mkString.split('\n').toList.sortWith(_ < _)
			for(target <- strList){
				val tmpRes = getSimilarString(target, strList, threshold)
				similarList.clear
				similarList += target
				for(similarStr <- tmpRes){
					similarList += similarStr
				}
				dictionary += similarList.toList
			}
			println("Finish.")
			return dictionary.toList
		} catch {
		  	case e: Exception => {
		  		println("Error in createDictionary(): " + e)
		  		return Nil
		  	}
		}	
	}
	def writeToFile(path: String, dictionary: List[List[String]]) = {
		// Write to dictionary to file on dist. After this, it is not neccessary to calculate the 
		// dictionary again with function createDictionary() again, just use readFromFile() to read 
		// it from disk.
		val filewriter = new FileWriter(path, false)
		val bufferedwriter = new BufferedWriter(filewriter)

		for(i <- 0 until dictionary.size) {
			for( j <- 0 until dictionary.apply(i).size) {
				bufferedwriter.write(dictionary.apply(i).apply(j))
				if(j < dictionary.apply(i).size-1){
					bufferedwriter.write(" ")
				}
			}
			if(i < dictionary.size){
				bufferedwriter.write("\n")
			}
		}
		bufferedwriter.close
	}
	def readFromFile(path: String): List[List[String]] = {
		// Read a complete dictionary from file. Don't need to calculate the edit distance between 
		// each pair of them. 
		try { 
			val lines = scala.io.Source.fromFile(path).mkString.split('\n').toList
			val dictionary = new scala.collection.mutable.ArrayBuffer[List[String]]()
			for (line <- lines){
				val strList = line.split(' ').toList
				dictionary += strList
			}
			return dictionary.toList
		} catch {
		  	case e: Exception => {
		  		println("Error in readFromFile(): "+e)
		  		return Nil
		  	}
		}
	}

	def print(dictionary: List[List[String]]) = {
		for( i <- 0 until dictionary.size) {
			for(j <- 0 until dictionary.apply(i).size){
				printf(dictionary.apply(i).apply(j))
				if(j < dictionary.apply(i).size-1)
					printf(" ")
			}
			if(i < dictionary.size-1)
				println()
		}
		println()
	}

	def binary_sesearch(target: String, dictionary: List[List[String]], low: Int, high: Int): Int = {
		val mid = (low + high)/2
		if(low > high)
			return -1
		else{
			if(dictionary.apply(mid).first == target)
				return mid
			else if(dictionary.apply(mid).first > target)
				return binary_sesearch(target, dictionary, low, mid-1)
			else
				return binary_sesearch(target, dictionary, mid+1, high)
		}
	}
}
/*
object test {
	def main(args: Array[String]): Unit = {
	 
	 	val dict = new Dictionary().createDictionary("./weblist/top1000", 2)
	 	new Dictionary().print(dict)
	 	new Dictionary().writeToFile("./weblist/top1000.dict", dict)
	/*
		val dict = new Dictionary().readFromFile("./weblist/top1000.dict")
		new Dictionary().print(dict)
		new Dictionary().writeToFile("./weblist/top1000.dict_2", dict)
	*/
	}
}*/