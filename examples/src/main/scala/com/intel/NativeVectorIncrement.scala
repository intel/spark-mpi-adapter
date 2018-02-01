import org.apache.spark._
import org.apache.spark.rdd.RDD
import com.intel.MPIAdapterForSpark.SparkNativeInterface

package com.intel.MPIAdapterForSpark
{
  object NativeVectorIncrement
  {
    def run(sc : org.apache.spark.SparkContext, 
      myrdd: org.apache.spark.rdd.RDD[Int]) =
    {
      val inputSize = (edges : Array[Int]) => { 4 + 4 * edges.length }
      val inputWrite = (edges : Array[Int], stream : java.nio.ByteBuffer) => {
        var cnt = 0
        val maxcnt = edges.length
        stream.putInt(maxcnt)
        while(cnt < maxcnt)
        {
          stream.putInt(edges(cnt))
          cnt = cnt + 1
        }
      }

      val outputRead = (stream : java.nio.ByteBuffer) => {
        stream.getInt()
      }

      // Copy data to shared memory
      val nativeData = SparkNativeInterface.RDDToNative(myrdd, inputWrite, inputSize)

      // Create ID for output data
      val nativeOutputs = SparkNativeInterface.EmptyNative(nativeData)

      // Run MPI
      SparkNativeInterface.runMPI(nativeData, Array(nativeOutputs),
        sys.env("MPI_ADAPTER_FOR_SPARK_EXAMPLES").concat("/vector-increment"), Array[String]())

      // Delete input data from shared memory
      SparkNativeInterface.delete(nativeData)

      // Copy output data from shared memory to HDFS
      val HDFSPath = SparkNativeInterface.NativeToHDFS(nativeOutputs)

      // Delete output data from shared memory
      SparkNativeInterface.delete(nativeOutputs)

      // Deserialize output data and copy into RDD
      SparkNativeInterface.NativeToRDD(sc, 
        outputRead, HDFSPath, (nativeData._2).toInt, 4)

    }
  }
}
