package com.cloudera.sa.example.sparkstreaming.sessionization

import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.streaming._
import org.apache.spark.streaming.dstream.PairDStreamFunctions
import org.apache.spark.streaming.StreamingContext._
import org.apache.spark.streaming.Seconds
import org.apache.spark.streaming.dstream.DStream
import org.apache.hadoop.io.LongWritable
import java.text.SimpleDateFormat
import org.apache.hadoop.hbase.HBaseConfiguration
import org.apache.spark.hbase.HBaseContext
import org.apache.hadoop.fs.Path
import org.apache.hadoop.hbase.client.Put
import org.apache.hadoop.hbase.util.Bytes
import org.apache.spark.streaming.dstream.FileInputDStream
import org.apache.hadoop.io.Text
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat
import scala.collection.immutable.HashMap
import java.util.Date

object SessionizeData {

  val OUTPUT_ARG = 1
  val HTABLE_ARG = 2
  val HFAMILY_ARG = 3
  val CHECKPOINT_DIR_ARG = 4
  val FIXED_ARGS = 5

  val SESSION_TIMEOUT = (60000 * 0.5).toInt

  val TOTAL_SESSION_TIME = "TOTAL_SESSION_TIME"
  val UNDER_A_MINUTE_COUNT = "UNDER_A_MINUTE_COUNT"
  val ONE_TO_TEN_MINUTE_COUNT = "ONE_TO_TEN_MINUTE_COUNT"
  val OVER_TEN_MINUTES_COUNT = "OVER_TEN_MINUTES_COUNT"
  val NEW_SESSION_COUNTS = "NEW_SESSION_COUNTS"
  val TOTAL_SESSION_COUNTS = "TOTAL_SESSION_COUNTS"
  val EVENT_COUNTS = "EVENT_COUNTS"
  val DEAD_SESSION_COUNTS = "DEAD_SESSION_COUNTS"
  val REVISTE_COUNT = "REVISTE_COUNT"

  val dateFormat = new SimpleDateFormat("dd/MMM/yyyy HH:mm:ss Z")

  def main(args: Array[String]) {
    if (args.length == 0) {
      println("SessionizeData {sourceType} {outputDir} {source information}");
      println("SessionizeData file {outputDir} {table} {family}  {hdfs checkpoint directory} {source file}");
      println("SessionizeData newFile {outputDir} {table} {family}  {hdfs checkpoint directory} {source file}");
      println("SessionizeData socket {outputDir} {table} {family}  {hdfs checkpoint directory} {host} {port}");
      return ;
    }

    val outputDir = args(OUTPUT_ARG)
    val hTable = args(HTABLE_ARG)
    val hFamily = args(HFAMILY_ARG)
    val checkpointDir = args(CHECKPOINT_DIR_ARG)

    val sparkConf = new SparkConf().setAppName("SessionizeData " + args(0))
    sparkConf.set("spark.cleaner.ttl", "120000");
    val sc = new SparkContext(sparkConf)

    val ssc = new StreamingContext(sc, Seconds(10))

    val conf = HBaseConfiguration.create();
    conf.addResource(new Path("/etc/hbase/conf/core-site.xml"));
    conf.addResource(new Path("/etc/hbase/conf/hbase-site.xml"));

    val hbaseContext = new HBaseContext(sc, conf);

    var lines: DStream[String] = null

    //Options for data load.  Will be adding Kafka and Flume at some point
    if (args(0).equals("socket")) {
      val host = args(FIXED_ARGS);
      val port = args(FIXED_ARGS + 1);

      println("host:" + host)
      println("port:" + Integer.parseInt(port))

      lines = ssc.socketTextStream(host, port.toInt)
    } else if (args(0).equals("file")) {

      val directory = args(FIXED_ARGS)
      println("directory:" + directory)
      lines = ssc.textFileStream(directory)
    } else if (args(0).equals("newFile")) {

      val directory = args(FIXED_ARGS)
      println("directory:" + directory)
      //ssc.textFileStream(directory)
      lines = ssc.fileStream[LongWritable, Text, TextInputFormat](directory, (t: Path) => true, true).map(_._2.toString)
    } else {
      throw new RuntimeException("bad type")
    }

    val ipKeyLines = lines.map[(String, (Long, Long, String))](t => {
      val time = dateFormat.parse(t.substring(t.indexOf('[') + 1, t.indexOf(']'))).getTime()
      (t.substring(0, t.indexOf(' ')), (time, time, t))
    })

    ipKeyLines.saveAsTextFiles(outputDir + "/ipKeyLines", "txt")

    val latestSessionInfo = ipKeyLines.reduceByKey((a, b) => {
      (Math.min(a._1, b._1), Math.max(a._2, b._2), "")
    }).updateStateByKey(updateStatbyOfSessions)

    //remove old sessions
    val onlyActiveSessions = latestSessionInfo.filter(t => System.currentTimeMillis() - t._2._2 < SESSION_TIMEOUT)

    val totals = onlyActiveSessions.mapPartitions[(Long, Long, Long, Long)](it =>
      {
        var totalSessionTime: Long = 0
        var underAMinuteCount: Long = 0
        var oneToTenMinuteCount: Long = 0
        var overTenMinutesCount: Long = 0

        it.foreach(a => {
          val time = a._2._2 - a._2._1
          totalSessionTime += time
          if (time < 60000) underAMinuteCount += 1
          else if (time < 600000) oneToTenMinuteCount += 1
          else overTenMinutesCount += 1
        })

        Iterator((totalSessionTime, underAMinuteCount, oneToTenMinuteCount, overTenMinutesCount))
      }, true).reduce((a, b) => {
      //totalSessionTime, underAMinuteCount, oneToTenMinuteCount, overTenMinutesCount
      (a._1 + b._1, a._2 + b._2, a._3 + b._3, a._4 + b._4)
    }).map[HashMap[String, Long]](t => HashMap((TOTAL_SESSION_TIME, t._1), (UNDER_A_MINUTE_COUNT, t._2), (ONE_TO_TEN_MINUTE_COUNT, t._3), (OVER_TEN_MINUTES_COUNT, t._4)))

    val newSessionCount = onlyActiveSessions.filter(t => System.currentTimeMillis() - t._2._2 < 11000 && t._2._4).
      count.map[HashMap[String, Long]](t => HashMap((NEW_SESSION_COUNTS, t)))
    
      val totalSessionCount = onlyActiveSessions.count.map[HashMap[String, Long]](t => HashMap((TOTAL_SESSION_COUNTS, t)))
    
    val totalEventsCount = ipKeyLines.count.map[HashMap[String, Long]](t => HashMap((EVENT_COUNTS, t)))
    
    val deadSessionsCount = latestSessionInfo.filter(t => {
      val gapTime = System.currentTimeMillis() - t._2._2
      gapTime > SESSION_TIMEOUT && gapTime < SESSION_TIMEOUT + 11000
    }).count.map[HashMap[String, Long]](t => HashMap((DEAD_SESSION_COUNTS, t)))

    val allCounts = newSessionCount.union(totalSessionCount).union(totals).union(totalEventsCount).union(deadSessionsCount).reduce((a, b) => b ++ a)

    hbaseContext.streamBulkPut[HashMap[String, Long]](allCounts, hTable, (t) => {
      val put = new Put(Bytes.toBytes("C." + (Long.MaxValue - System.currentTimeMillis())))
      t.foreach(kv => put.add(Bytes.toBytes(hFamily), Bytes.toBytes(kv._1), Bytes.toBytes(kv._2.toString)))
      put
    }, false)

    //HDFS Ingestion
    ipKeyLines.join(onlyActiveSessions).map(t => {
      //Session time start | Event message 
      dateFormat.format(new Date(t._2._2._1)) + "\t" + t._2._1._3
    }).saveAsTextFiles(outputDir + "/session", "txt")

    ssc.checkpoint(checkpointDir)

    ssc.start
    ssc.awaitTermination
  }

  def updateStatbyOfSessions(a: Seq[(Long, Long, String)], b: Option[(Long, Long, Long, Boolean)]): Option[(Long, Long, Long, Boolean)] = {
    var result: Option[(Long, Long, Long, Boolean)] = null

    if (a.size == 0) {
      if (System.currentTimeMillis() - b.get._2 < SESSION_TIMEOUT + 11000) {
        result = None
      } else {
        result = b
      }
    }

    a.foreach(c => {
      if (b.isEmpty) {
        result = Some((c._1, c._2, 1, true))
      } else {
        if (c._1 - b.get._2 < SESSION_TIMEOUT) {
          result = Some((Math.min(c._1, b.get._1), Math.max(c._2, b.get._2), b.get._3, false))
        } else {
          result = Some((c._1, c._2, b.get._3 + 1, true))
        }
      }
    })
    result
  }
}