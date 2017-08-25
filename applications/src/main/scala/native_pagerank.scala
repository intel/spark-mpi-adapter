import org.apache.spark._
import org.apache.spark.rdd.RDD
import com.intel.ClosingTheGap.SparkNativeInterface

package com.intel.ClosingTheGap
{
  object NativePagerank
  {
    def run(sc : org.apache.spark.SparkContext, myrdd: org.apache.spark.rdd.RDD[(Int,Int,Int)], numIter : Int, shouldFail : Int) =
    {
      val inputSize = (edges : Array[(Int, Int, Int)]) => { 4 + 12 * edges.length }
      val inputWrite = (edges : Array[(Int, Int, Int)], stream : java.nio.ByteBuffer) => {
        var cnt = 0
	val maxcnt = edges.length
	stream.putInt(maxcnt)
	while(cnt < maxcnt)
	{
          stream.putInt(edges(cnt)._1)
          stream.putInt(edges(cnt)._2)
          stream.putInt(edges(cnt)._3)
	  cnt = cnt + 1
	}
      }

      val outputRead = (stream : java.nio.ByteBuffer) => {
        val vid = stream.getInt()
        val pagerank = stream.getDouble()
        (vid, pagerank)
      }

      // Copy data to shared memory
      val t1 = System.currentTimeMillis

      val nativeData = SparkNativeInterface.RDDToNative(myrdd, inputWrite, inputSize)

      val t2 = System.currentTimeMillis
      println("Copy to shm Time: " + ((t2-t1)/1000.0).toString)

      // Create ID for output data
      val nativeOutputs = SparkNativeInterface.EmptyNative(nativeData)

      // Run MPI
      SparkNativeInterface.runMPI(nativeData, Array(nativeOutputs), "/home/anderso2/ClosingTheGap/bin/Pagerank", Array[String](numIter.toString, 0.toString))

      // Delete input data from shared memory
      SparkNativeInterface.delete(nativeData)

      val t3 = System.currentTimeMillis
      // Fail on first iteration
      if(shouldFail == 1)
      {
        println("Copy to shm Time: " + ((t2-t1)/1000.0).toString)
        println("Run MPI Time: " + ((t3-t2)/1000.0).toString)

        // Kill a node, then force recomputation
	println("Killing process")
        import sys.process._
	val commandStr1 = "ssh pcl-me60 sudo /usr/bin/pkill -f spark"
	val commandStr2 = "ssh pcl-me60 sudo /usr/bin/pkill -f hadoop" 
	commandStr1 .!
	commandStr2 .!
        Thread.sleep(10000)
        throw new java.io.FileNotFoundException
      }

      // Copy output data from shared memory to HDFS
      val HDFSPath = SparkNativeInterface.NativeToHDFS(nativeOutputs)

      val t4 = System.currentTimeMillis

      // Delete output data from shared memory
      SparkNativeInterface.delete(nativeOutputs)

      // Deserialize output data and copy into RDD
      val output = SparkNativeInterface.NativeToRDD(sc, outputRead, HDFSPath, 44 * (nativeData._2).toInt, 12)
                                       .repartition(44 * (nativeData._2).toInt).cache
      output.count

      val t5 = System.currentTimeMillis

      println("Run MPI Time: " + ((t3-t2)/1000.0).toString)
      println("Write to HDFS Time: " + ((t4-t3)/1000.0).toString)
      println("Read from HDFS Time: " + ((t5-t4)/1000.0).toString)

      // Return output RDD
      output
    }
  }
}
