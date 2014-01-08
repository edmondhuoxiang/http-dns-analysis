package org.edmond.Dictionary

import org.apache.log4j.Logger
import org.apache.log4j.Level

import spark.SparkContext
import spark.SparkContext._

import scala.io.Source
import sacal.io._
import java.io.File
import java.io.FileWriter
import java.io.BufferedWriter

import org.edmond.DLDistance._

class Dictionary(){
	def getSimilarString(target: String, strList: List[String], threshold: Int): List[String] = {
		// Get all similar strings from a string list when a target string is given. 
		// Compared with target string, the strings in the result list should have edit distance 
		// less than threshold but more than zero
		val similarSet = new scala.collection.mutable.ArrayBuilder[String]()

		for( str <- strList) {
			val dist = new DLDistance().distance(target, str)
			if(dist < threshold && dist > 0){
				similarSet += str
			}
		}
		return similarSet.toList
	}

	def createDictionary(path: String) = {
		// Create the Discrionary structure in memomery when a path is given. 
		// The input file should store all possible domains. 	
	}
	def writeToFile(path: String) = {
		// Write to disctionary to file on dist. After this, it is not neccessary to calculate the 
		// distionary again with function createDictionary() again, just use readFromFile() to read 
		// it from disk.
		
	}
	def readFromFile(path: String) = {
		// Read a complete dictionary from file. Don't need to calculate the edit distance between 
		// each pair of them. 	
	}
}