package org.edmond.DLDistance

import org.apache.hadoop.filexache.DistributedCache
import scala.math._

import spark.SparkContext
import spark.SparkContext._

import org.apache.log4j.Logger
import org.apache.log4j.Level

import scala.io.Source
import scala.io._
import java.io.File

/* 	
	Implementation of pseudocode found at
	http://en.wikipedia.org/wiki/Damerau%E2%80%93Levenshtein_distance
	for Damerau-Levenschtein edit distance.

	Use `new DLDistance().distance()` can get the edit distance between
	two strings. 

	Use `new DLDistance().keyboard_distance()` can get the edit distance 
	on key-board between two strings. 

	Use `new DLDistance().typoCandidate()` can get all possible typos from 
	a given string. The distance between typos and correct string is no more
	than 2.
*/

class DLDistance{
	def minimum(i1: Int, i2: Int, i3: Int) = min(min(i1, i2), i3)

	def distance(s1:String, s2:String) = {
		val dist=Array.tabulate(s1.length+1, s2.length+1){(j,i)=>if(j==0) i else if (i==0) j else 0}
		for(j<-1 to s2.length; i<-1 to s1.length){
         	val cost = if(s1(i-1) == s2(j-1)) 0 else 1

         	dist(i)(j)=minimum(dist(i-1)(j)+1,dist(i)(j-1)+1,dist(i-1)(j-1)+cost)

         	if(i > 1 && j > 1 && s1(i-1)==s2(j-2) && s1(i-2)==s2(j-1)){
         	   dist(i)(j) = min(dist(i)(j),dist(i-2)(j-2)+cost)
         	}
       	}

       	dist(s1.length)(s2.length)
	}

	//val keyboard = initialize();
  	def initialize(): scala.collection.mutable.HashMap[Char, Array[Char]] = {
    	val lines = scala.io.Source.fromFile("/Users/edmond/Typosquatting/src/main/scala/keyboard-dist").getLines.toList
    	//val lines = scala.io.Source.fromFile("/home/xiang/Typosquatting/src/main/scala/keyboard-dist").getLines.toList
    	val set = new scala.collection.mutable.HashMap[Char, Array[Char]]()
    	for(line <- lines){
    		val char = line.split(' ').apply(0).charAt(0)
      		val tmpBuffer = scala.collection.mutable.ArrayBuffer[Char]()
      		for (c <- line.split(' ')){
        		if(c.charAt(0) != char){
          			tmpBuffer.+=:(c.charAt(0))
        		}
      		}
      		set.+=((char, tmpBuffer.toArray))
    	}
    	return set
  	}

  	def minimum(i1: Float, i2: Float, i3: Float)=min(min(i1, i2), i3)

  	def keyboard_distance(s1:String, s2:String)={
    	val keyboard = initialize();
        val dist=Array.tabulate(s1.length+1, s2.length+1){(j,i)=>if(j==0) i.toFloat else if (i==0) j.toFloat else 0.toFloat}

        for(j<-1 to s2.length; i<-1 to s1.length){
          	var cost = if(s1(i-1) == s2(j-1)) 0.toFloat else 1.toFloat
          	val arr = keyboard(s1(i-1))

          	for(c <- arr){
            	if(c == s2(j-1)){
            	  	cost = cost - 0.5.toFloat
            	}
          	}
          	val deletion = dist(i-1)(j)+1
          	val insertion = dist(i)(j-1)+1
          	val substitution = dist(i-1)(j-1)+cost

          	dist(i)(j)=minimum(deletion, insertion, substitution)

          	if(i > 1 && j > 1 && s1(i-1)==s2(j-2) && s1(i-2)==s2(j-1)){
            	dist(i)(j) = min(dist(i)(j).toFloat,dist(i-2)(j-2).toFloat+cost)
          	}
        }
        dist(s1.length)(s2.length)
    }

    def insert(originStr: String, pos: Int): Array[String] ={
      	val insertArray = new scala.collection.mutable.ArrayBuffer[String]()
      	val insertStr = new scala.collection.mutable.StringBuilder()
      	for (i <- 0 until 26){
        	insertStr.append(originStr)
        	val char = 'a'+i
        	insertStr.insert(pos, char.asInstanceOf[Char])
        	insertArray += insertStr.toString
        	insertStr.clear()
      	}
      	return insertArray.toArray
    } 

    def del(originStr: String, pos: Int): String ={
      	val deletionStr = new scala.collection.mutable.StringBuilder()
      	deletionStr.append(originStr)
      	deletionStr.deleteCharAt(pos)
      	return deletionStr.toString
    }   

    def subs(originStr: String, pos: Int): Array[String]= {
      	val substitutionArray = new scala.collection.mutable.ArrayBuffer[String]()
      	val substitutionStr = new scala.collection.mutable.StringBuilder()
      	for(i <- 0 until 26){
        	substitutionStr.clear()
        	substitutionStr.append(originStr)
        	val char = 'a' + i
        	val c = originStr.charAt(pos)
        	if (c!=char){
          		substitutionStr.deleteCharAt(pos)
          		substitutionStr.insert(pos, char.asInstanceOf[Char])
          		substitutionArray += substitutionStr.toString
        	}
      	}
      	for(i <- 0 until 10){
        	substitutionStr.clear()
        	substitutionStr.append(originStr)
        	val num = '0' + i
        	val c = originStr.charAt(pos)
        	if(c!=num){
          		substitutionStr.deleteCharAt(pos)
          		substitutionStr.insert(pos, num.asInstanceOf[Char])
          		substitutionArray += substitutionStr.toString
        	}
      	}
      	if(originStr.charAt(pos)=='.'){
        	substitutionStr.clear()
        	substitutionStr.append(originStr)
        	substitutionStr.deleteCharAt(pos)
        	substitutionStr.insert(pos,','.asInstanceOf[Char])
        	substitutionArray += substitutionStr.toString
      	}
      	return substitutionArray.toArray
    }

    def trans(originStr: String): Array[String] = {
      	val transArray = new scala.collection.mutable.ArrayBuffer[String]()
      	val transStr = new scala.collection.mutable.StringBuilder()
      	for(i <- 0 until originStr.length-1){
        	if(originStr.charAt(i)!=originStr.charAt(i+1)){    
          		transStr.append(originStr)
          		val char = originStr.charAt(i)
          		transStr.deleteCharAt(i)
          		transStr.insert(i+1, char)
          		transArray += transStr.toString
          		transStr.clear()
        	}
      	}
      	return transArray.toArray
    }

    def typoCandidate(originStr: String): Array[String] = {
      	val result = new scala.collection.mutable.ArrayBuffer[String]()
      
      	//all possible typo with insertion
      	for(i <- 0 until originStr.length+1){
        	val arr = insert(originStr, i)
        	result.appendAll(arr)
      	}

      	//all possible typo with deletion
      	for(i <- 0 until originStr.length){
        	val str = del(originStr, i)
        	result.append(str)
      	}

      	//all possible typo with substitution
      	for(i <- 0 until originStr.length){
        	val arr = subs(originStr, i)
        	result.appendAll(arr)
      	}
      	//all possible type with transposition
      	val arr = trans(originStr)
      	result.appendAll(arr)
    	return result.distinct.toArray
  	}
}

