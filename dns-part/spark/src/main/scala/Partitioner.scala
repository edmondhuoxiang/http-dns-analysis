package org.edmond.Partitioner

import spark.Partitioner
import spark.rdd
/**
 * An object that defines how the elements in a key-value pair RDD are partitioned by key.
 * Maps each key to a Partition ID, from 0 to `numPartitions - 1`.
 */
/*abstract class Partitioner extends Serializable {
        def numPartitions: Int
        def getPartition(key: Any): Int
}

object Partitioner {
  /**
   * Choose a partitioner to use for a cogroup-like operation between a number of RDDs.
   *
   * If any of the RDDs already has a partitioner, choose that one.
   *
   * Otherwise, we use a default HashPartitioner. For the number of partitions, if
   * spark.default.parallelism is set, then we'll use the value from SparkContext
   * defaultParallelism, otherwise we'll use the max number of upstream partitions.
   *
   * Unless spark.default.parallelism is set, He number of partitions will be the
   * same as the number of partitions in the largest upstream RDD, as this should
   * be least likely to cause out-of-memory errors.
   *
   * We use two method parameters (rdd, others) to enforce callers passing at least 1 RDD.
   */

   def deaultPartitioner(rdd: spark.RDD[_], other: spark.RDD[_]*): Partitioner = {
           val bySize = (Seq(rdd) ++ other).sortBy(_.partitions.size).reverse
           if(System.getProperty("spark.default.parallelism") != null) {
                   return new HashPartitioner(rdd.context.defaultParallelism)
           }else {
                   return new HashPartitioner(bySize.head.partitions.size)
           }
   }
}*/

/**
 * A [[spark.Partitioner]] that implements hash-based partitioning using Java's `Object.hashCode`.
 * 
 * Java arrays have hashCode that are based on the arrays' identities rather than their contents,
 * so attempting to partitions an RDD[Array[_]] or RDD[(Array[_], _)] using a HashPartitioner will
 * produce an unexpected or incorrect result.
 **/
class HashPartitioner(partitions: Int) extends Partitioner {
         def numPartitions = partitions

         def getPartition(key: Any): Int = {
                 //println("key: " + key)
                 if(key == null){
                         return 0
                 } else {
                         val mod = key.hashCode % partitions
                         if (mod < 0) {
                                 mod + partitions
                         } else {
                                 mod // Guard against negative has codes
                         }
                 }
         }

         override def equals(other: Any): Boolean = other match {
                 case h: HashPartitioner => 
                         h.numPartitions == numPartitions
                 case _ =>
                         false
         }
}