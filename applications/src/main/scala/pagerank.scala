import org.apache.spark._
import org.apache.spark.rdd.RDD
import com.intel.ClosingTheGap.NativePagerank
import org.apache.spark.graphx._

package com.intel.ClosingTheGap
{
  object Pagerank
  {
    def main(args : Array[String])
    {
      if(args.size < 5)
      {
        println("Usage: Pagerank filename nexecutors npartitions niter spark/native/jni [partitionstrategy]")
        System.exit(1)
      }
      val filename = args(0)
      val numExecutors = args(1).toInt
      val numPartitions = args(2).toInt
      val numIter = args(3).toInt
      val implementation = args(4)
      val partitionStrategy = args(5)
      println("Num executors: " + numExecutors.toString)
      println("Num partitions: " + numPartitions.toString)
      println("Num iter : " + numIter.toString)
      println("implementation: " + implementation.toString)
      println("partitionStrategy: " + partitionStrategy.toString)
      val sparkConf = new SparkConf().setAppName("Pagerank").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      /*
                                                            .set("spark.dynamicAllocation.enabled", "true")
							    .set("spark.dynamicAllocation.minExecutors", numExecutors.toString)
							    .set("spark.dynamicAllocation.maxExecutors", numExecutors.toString)
							    .set("spark.dynamicAllocation.initialExecutors", numExecutors.toString)
							    .set("spark.network.timeout", "360s")
							    */
      //val sparkConf = new SparkConf().setAppName("Pagerank").set("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
      org.apache.spark.graphx.GraphXUtils.registerKryoClasses(sparkConf)
      val sc = new SparkContext(sparkConf)
      Thread.sleep(10000)

      println("Default partitions: " + sc.defaultMinPartitions);
      println("Default parallelism: " + sc.defaultParallelism);

      if(implementation == "native")
      {
        val load_start_time = System.currentTimeMillis
	println("load_start_time " + load_start_time.toString)
        var facebook = sc.textFile(filename, numPartitions.toInt).map(s=> { val twostr = s.split(" "); ((twostr(0).toInt, twostr(1).toInt, 1.toInt)) }).cache
        println(facebook.count)
        val load_end_time = System.currentTimeMillis
	println("load_end_time " + load_end_time.toString)

        val outer_start_time = System.currentTimeMillis
	println("outer_start_time " + outer_start_time.toString)

        var cnt = 0
        var isDone = false
        while(isDone == false)
        {
          try
          {
	     var shouldFail = 0
	     if(cnt == 0)
	     {
	       shouldFail = 0
	     }
             val pageranks = NativePagerank.run(sc, facebook, numIter, shouldFail).cache
             println(pageranks.count)
             val outer_end_time = System.currentTimeMillis
             println("outer_end_time " + outer_end_time.toString)

             val max = pageranks.map(x=>x._2).reduce((x1,x2)=>Math.max(x1,x2)) 
             val tmpdir = "/btg/native_pagerank/" + java.util.Calendar.getInstance().getTime.toString.replaceAll(" ", "_").replaceAll(":","_")
    	     println("tmpdir: " + tmpdir.toString)
             val native_pageranks = pageranks.map(x=>(x._1, x._2)).sortByKey().map(x=>x._1.toString + " " + x._2.toString)
             //native_pageranks.saveAsTextFile(tmpdir)
             println("max: " + max.toString)
             println("Load Time: " + ((load_end_time-load_start_time)/1000.0).toString)
             println("Outer Time: " + ((outer_end_time-outer_start_time)/1000.0).toString)
             isDone = true
          }
          catch
          {
            case ex: java.io.FileNotFoundException => { println("Caught!") }
          }
          finally
          {
            println("Done!")
          } 
	  cnt = cnt + 1
        }
      }
    }
  }
}
