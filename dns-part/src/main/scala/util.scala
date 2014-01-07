package org.edmond.util

import java.util.Date
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.io.File
import scala.util.matching.Regex

import org.edmond.DLDistance._

class parseUtils{
	def parseDomain(domain_1: String, domain_2: String): String = {
		val num = domain_2.count(_ == '.') + 1
		val num_1 = domain_1.count(_ == '.')
		if(num_1 == num)
			return domain_1
		val parts = domain_1.split('.').zipWithIndex
		val len = parts.length
		val res = parts.filter(r => r._2 > (len - num - 1)).map(r => r._1)
		return res.mkString(".")+"."
	}


	def convertStampToDate(timestamp: Int) : java.util.Date = {
		//Convert timestamp to Date, the paramete timestamp is in second rather than millisecond
		val stamp = new java.sql.Timestamp(timestamp.toLong * 1000) // convert timestamp to Milliseconds
		val date = new java.util.Date(stamp.getTime())
		return date
	}

	def lookUpString(target: String, sortedArr: Array[String], start: Int, end: Int): Int = {
        /*        println("target: "+target)
                println("LENGTH: " + sortedArr.length)
                println("start: "+start)
                println("end: "+end)
        */       
        if(start == end){
        	val tmp = parseDomain(target, sortedArr.apply(start)+".")
            val distance = new DLDistance().distance(tmp, sortedArr.apply(start)+".")
            if( distance <=2 ){
                return start
            } else {
               	return -1
            }
        }
        else if( start > end){
          	return -1
        }
        var curIndex = -1
        curIndex = (start + end) / 2;
        //println("curIndex: "+curIndex)
        var middleValue = sortedArr.apply(curIndex)
        //println("middleValue: " + middleValue)
        val tmp_target = parseDomain(target, middleValue + ".")
        val dist = new DLDistance().distance(tmp_target, middleValue)
        if (dist <= 2){
          	return curIndex
        }
        else if(target < middleValue){
           	return lookUpString(target, sortedArr, start, curIndex-1)
        }
        else{
           	return lookUpString(target, sortedArr, curIndex+1, end)
        }
    }
}

class ListFiles(){
	def recursiveListFiles(f: File) : Array[File] = {
		val these = f.listFiles
		these ++ these.filter(_.isDirectory).flatMap(recursiveListFiles(_))
	}

	def recursiveListFiles(f: File, r: Regex): Array[File] = {
		val these = f.listFiles
		val good = these.filter(f=>r.findFirstIn(f.getName).isDefined)
		good ++ these.filter(_.isDirectory).flatMap(recursiveListFiles(_,r))
	}
}