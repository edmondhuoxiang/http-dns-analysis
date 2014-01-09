package org.edmond.util

import java.util.Date
import java.sql.Timestamp
import java.text.SimpleDateFormat
import java.io.File
import scala.util.matching.Regex

import org.edmond.DLDistance._

class parseUtils{
	def parseDomain(domain_1: String, domain_2: String): String = {
		val num = domain_2.count(_ == '.') 
		val num_1 = domain_1.count(_ == '.')
		if(num_1 == num)
			return domain_1
		val parts = domain_1.split('.').zipWithIndex
		val len = parts.length
		val res = parts.filter(r => r._2 > (len - num - 2)).map(r => r._1)
		return res.mkString(".")
	}


	def convertStampToDate(timestamp: Int) : java.util.Date = {
		//Convert timestamp to Date, the paramete timestamp is in second rather than millisecond
		val stamp = new java.sql.Timestamp(timestamp.toLong * 1000) // convert timestamp to Milliseconds
		val date = new java.util.Date(stamp.getTime())
		return date
	}

	def formatDomainName(domain: String): String = {
		val res = new scala.collection.mutable.StringBuilder()
		val arr = domain.toLowerCase.split('.')
		if(arr.first != "www"){
			res ++= arr.first + "."
		}
		for(i <- 1 until arr.length){
			res ++= arr.apply(i)
			if(i != arr.length - 1)
				res ++= "."
		}
		return res.toString
	}

	def lookUpString(Target: String, sortedArr: Array[String], start: Int, end: Int): Int = {
        /*        println("target: "+target)
                println("LENGTH: " + sortedArr.length)
                println("start: "+start)
                println("end: "+end)
        */ 
        val target = formatDomainName(Target)
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
        println("target: "+tmp_target)
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

    def lookUpInDict(Target: String, dict: List[List[String]], start: Int, end: Int, threshold: Int): Int = {
    	val target = formatDomainName(Target)
    	if(start == end){
    		val tmp = parseDomain(target, dict.apply(start).apply(0)) //?
    		val distance = new DLDistance().distance(tmp, dict.apply(start).first)
    		if(distance <= threshold){
    			return start
    		}else{
    			return -1
    		}
    	}
    	else if(start > end){
    		return -1
    	}

    	var curIndex = -1
    	curIndex = (start + end ) / 2
    	var middleValue = dict.apply(curIndex).first
    	var tmp_target = parseDomain(target, middleValue)
    	//println("tmp_target: "+ tmp_target+ "   middleValue:"+middleValue)
    	val dist  = new DLDistance().distance(tmp_target, middleValue)
    	if(dist <= threshold){

    		return curIndex
    	}
    	else if(tmp_target < middleValue){
    		return lookUpInDict(target, dict, start, curIndex-1, threshold)
    	}
    	else{
    		return lookUpInDict(target, dict, curIndex+1, end, threshold)
    	}
    }

    def getDistance(str1: String, str2: String): Int = {
    	val tmp = parseDomain(str1, str2)
    	val distance = new DLDistance().distance(tmp, str2)
    	return distance
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