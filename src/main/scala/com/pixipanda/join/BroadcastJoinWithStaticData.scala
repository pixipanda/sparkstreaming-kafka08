package com.pixipanda.join

import kafka.serializer.StringDecoder
import org.apache.spark.sql.{Row, SQLContext}
import org.apache.spark.sql.types.{StringType, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}
import org.apache.spark.{SparkContext, SparkConf}
import org.apache.spark.sql.functions._

object  BroadcastJoinWithStaticData {

  def getCustomerHashMap(file: String) = {

    val customerMap = scala.collection.mutable.Map.empty[String, Array[String]]

    val lines = scala.io.Source.fromFile(file).getLines()

    while (lines.hasNext) {
      val line = lines.next()
      val customer = Customer.parse(line)
      val fields = line.split(",")
      val key = fields(0)
      val value = fields.drop(1)
      customerMap.put(key, value)
    }
    customerMap
  }


  def main(args: Array[String]) {

    val masterOfCluster = args(0)
    val topic = args(1)
    val customerPath = args(2)

    val conf = new SparkConf()
      .setMaster(masterOfCluster)
      .setAppName("NetworkWordCount")
      .set("spark.streaming.blockInterval", "5000ms")

    val sparkContext = new SparkContext(conf)
    val sqlContext = new SQLContext(sparkContext)

    import sqlContext.implicits._

    val customerMap = getCustomerHashMap(customerPath)
    val customerBroadcast = sparkContext.broadcast(customerMap)

    val transactionSchema = new StructType()
      .add("cc_num", StringType, true)
      .add("trans_num", StringType, true)
      .add("trans_time", StringType, true)
      .add("category", StringType, true)
      .add("merchant", StringType, true)
      .add("amt", StringType, true)
      .add("merch_lat", StringType, true)
      .add("merch_long", StringType, true)

    val customerSchema = new StructType()
      .add("cc_num", StringType, true)
      .add("first", StringType, true)
      .add("last", StringType, true)
      .add("gender", StringType, true)
      .add("street", StringType, true)
      .add("city", StringType, true)
      .add("state", StringType, true)
      .add("zip", StringType, true)
      .add("lat", StringType, true)
      .add("long", StringType, true)
      .add("job", StringType, true)
      .add("dob", StringType, true)

    val ssc = new StreamingContext(sparkContext, Seconds(10))

    val kafkaParams = Map(
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> "SingleReceiverGroup",
      "zookeeper.connection.timeout.ms" -> "1000")

    val topicMap = Map[String, Int](topic -> 1)

    val kafkaStreams = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicMap, StorageLevel.MEMORY_ONLY_SER).map(_._2)

    kafkaStreams.foreachRDD(rdd => {

      if(!rdd.isEmpty()) {

        val transactionDF = rdd.toDF("value")
          .withColumn("transaction", from_json($"value", transactionSchema))
          .select("transaction.*")

        val transactionRdd = transactionDF.rdd
        val joinedRdd = transactionRdd.mapPartitions(rowItr => {
          val customerMap = customerBroadcast.value
          rowItr.map(row => {
            val cc_num = row.getAs[String]("cc_num")
            val customerFields = customerMap.getOrElse(cc_num, Array.empty)
            Row.fromSeq(row.toSeq  ++ customerFields )
          })

        })

        val joinedDF = sqlContext.createDataFrame(joinedRdd, StructType(transactionSchema.fields ++ customerSchema.fields.drop(1)))

        joinedDF.show(false)
      } else {
        println("Rdd is empty")
      }
    })
    ssc.start()
    ssc.awaitTermination()
  }
}