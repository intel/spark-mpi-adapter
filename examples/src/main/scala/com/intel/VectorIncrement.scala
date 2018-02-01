import org.apache.spark._
import com.intel.MPIAdapterForSpark.NativeVectorIncrement

package com.intel.MPIAdapterForSpark
{
  object VectorIncrement
  {
    def main(args : Array[String])
    {
      val sparkConf = new SparkConf().setAppName("VectorIncrement")
        .set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      val sc = new SparkContext(sparkConf)
      Thread.sleep(10000)

      var my_array = sc.parallelize(Array(100)).map(s=>1.toInt).cache
      val my_array_incremented = NativeVectorIncrement.run(sc, my_array).cache
      val my_array_ne_zero_count = my_array_incremented.filter(x=>x!=2).count
      if(my_array_ne_zero_count > 0)
      {
        println("FAILED\n");
      }
      else
      {
        println("PASSED\n");
      }
    }
  }
}
