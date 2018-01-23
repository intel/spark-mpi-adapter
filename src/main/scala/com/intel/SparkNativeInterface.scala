package com.intel.MPIAdapterForSpark
{
  object SparkNativeInterface extends Serializable
  {
    var IdCount = 0
    var hdfsBase = "/btg/intermediate"

    def getIP() =
    {
      import sys.process._
      ("hostname" !!).trim + "-opa"
    }

    val writeHostfile = (Id : Int, ips : Array[String]) =>
    {
      import java.net._
      import sys.process._
      import scala.collection.mutable._
      import java.io.PrintWriter
      import java.io.File
      val hostfile_stream = new PrintWriter(new File("/dev/shm/btg" + Id + "_" + "hostfile"))
      val ipaddr = getIP()
      hostfile_stream.write(ipaddr + "\n")
      for (ip <- ips) {
        hostfile_stream.write(ip + "\n")
      }
      hostfile_stream.close()
    }

    val writePartitionfile = (Id : Int, partitions_ranks : Array[(Int,Int,Int)]) => 
    {
      import java.net._
      import sys.process._
      import scala.collection.mutable._
      import java.io.PrintWriter
      import java.io.File
      val partitions_stream = new PrintWriter(new File("/dev/shm/btg" + Id + "_" + "partitions"))
      for (p_r <- partitions_ranks) {
        partitions_stream.write(p_r._1.toString + ',' + p_r._2.toString + "," + p_r._3.toString + "\n")
      }
      partitions_stream.close()
    }

    def RDDToNative[U:scala.reflect.ClassTag](myrdd : org.apache.spark.rdd.RDD[U],
      writer : (Array[U],java.nio.ByteBuffer)=>Unit, getNbytes : (Array[U])=>Int) =
    {
      def serialize_partition[U:scala.reflect.ClassTag](MyId: Int,
        writer : (Array[U],java.nio.ByteBuffer)=>Unit, getNbytes : (Array[U])=>Int, p : Iterator[U])   =
      {
        val arr = p.toArray
        val nbytes = getNbytes(arr)
        val partitionIndex = org.apache.spark.TaskContext.get().partitionId()
        val chan =
          (java.nio.file.Files.newByteChannel(java.nio.file.Paths.get("/dev/shm/btg"
            + MyId.toString + "_" + partitionIndex.toString),
          java.nio.file.StandardOpenOption.READ,
          java.nio.file.StandardOpenOption.WRITE,
          java.nio.file.StandardOpenOption.CREATE).asInstanceOf[java.nio.channels.FileChannel])
        val mbuf = chan.map(java.nio.channels.FileChannel.MapMode.READ_WRITE, 0, nbytes)
        mbuf.order(java.nio.ByteOrder.LITTLE_ENDIAN)
        writer(arr, mbuf)
        mbuf.force()
        chan.close()
        Array((SparkNativeInterface.getIP(),partitionIndex,nbytes)).iterator
      }
      val IdBcast = org.apache.spark.SparkContext.getOrCreate().broadcast(IdCount)
      val partitions = myrdd.mapPartitions(it=>serialize_partition(IdBcast.value, writer, getNbytes, it)).collect()
      var ips = partitions.map(x=>x._1).distinct
      var partitions_ranks = partitions.map(x=>(ips.indexWhere(_==x._1)+1, x._2, x._3))
      val Id = IdCount
      IdCount = IdCount + 1
      writeHostfile(Id, ips)
      writePartitionfile(Id, partitions_ranks)
      (Id, ips.size+1)
    }

    def EmptyNative(nativeData : (Int,Int)) =
    {
      val newId = IdCount
      IdCount = IdCount + 1
      (newId, nativeData._2)
    }

    def NativeToRDD[U : scala.reflect.ClassTag] (sc : org.apache.spark.SparkContext,
      reader : (java.nio.ByteBuffer)=>U, fname : String, numPartitions : Int, numBytes : Int) =
    {
      def NativeToRDD_fn [U :scala.reflect.ClassTag] (numBytes : Int,
        reader : (java.nio.ByteBuffer) =>U, p : (String, org.apache.spark.input.PortableDataStream)) =
      {
        val arr = java.nio.ByteBuffer.wrap(p._2.toArray())
        arr.order(java.nio.ByteOrder.LITTLE_ENDIAN)
        val arrlen = arr.array().length / numBytes
        var outarr = new Array[U](arrlen)
        var cnt = 0
        while(cnt < arrlen)
        {
          outarr(cnt) = reader(arr)
          cnt = cnt + 1
        }
        outarr.iterator
      }
      sc.binaryFiles(fname, numPartitions).flatMap(x=>NativeToRDD_fn(numBytes, reader, x))
    }

    def runMPI(nativeSrc : (Int,Int), nativeDsts : Array[(Int,Int)], binaryPath : String, extraArgs : Array[String]) =
    {
      import sys.process._
      val srcPath = "/dev/shm/btg" + nativeSrc._1.toString + "_"
      val dstPaths = nativeDsts.map(x=>"/dev/shm/btg" + x._1.toString + "_").mkString(" ")
      val commandStr = ("mpiexec -env OMP_NUM_THREADS 44 -env I_MPI_PIN_DOMAIN omp -env I_MPI_FABRICS=tmi -n "
        + nativeSrc._2 + " -hostfile " + srcPath + "hostfile -perhost 1 " + binaryPath + " " + srcPath + " " + dstPaths + " " + extraArgs.mkString(" "))
      println(commandStr)
      val retval = commandStr .!
      println("Retval1: " + retval.toString)
      if(retval != 0)
      {
        throw new java.io.FileNotFoundException
      }
      retval
    }

    def NativeToHDFS(nativeData : (Int,Int)) =
    {
      import sys.process._
      val tmpdir = hdfsBase + "/" + java.util.Calendar.getInstance().getTime.toString.replaceAll(" ", "_").replaceAll(":","_")
      val commandStr = ("mpiexec -env OMP_NUM_THREADS 44 -env I_MPI_PIN_DOMAIN omp -env I_MPI_FABRICS=tmi -n "
        + nativeData._2.toString + " -hostfile " + "/dev/shm/btg" + nativeData._1.toString + "_" + "hostfile -perhost 1 "
        + sys.env("MPI_ADAPTER_FOR_SPARK_HOME") + "/shm-to-hdfs" + " " + "/dev/shm/btg" + nativeData._1.toString + "_" + " " + tmpdir.toString)
      println(commandStr)
      val retval = commandStr .!
      if(retval != 0)
      {
        throw new java.io.FileNotFoundException
      }
      tmpdir
    }

    def delete(nativeData : (Int, Int)) =
    {
      import sys.process._
      val commandStr = ("mpiexec  -n " + nativeData._2.toString + " -hostfile " + "/dev/shm/btg"
        + nativeData._1.toString + "_" + "hostfile -perhost 1 " + sys.env("MPI_ADAPTER_FOR_SPARK_HOME") + "/mpiclear.sh " + nativeData._1.toString)
      println(commandStr)
      val retval = commandStr .!
      if(retval != 0)
      {
        throw new java.io.FileNotFoundException
      }
    }
  }
}
