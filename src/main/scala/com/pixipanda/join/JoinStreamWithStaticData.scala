package com.pixipanda.join

import kafka.serializer.StringDecoder
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.{TimestampType, DoubleType, StringType, StructType}
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


object JoinStreamWithStaticData {

  def main(args: Array[String]) {

    val masterOfCluster = args(0)
    val topic = args(1)
    val customerPath = args(2)

    /*val conf = new SparkConf()
      .setMaster(masterOfCluster)
      .setAppName("NetworkWordCount")
      .set("spark.streaming.blockInterval", "5000ms")*/


    val sparkSession = SparkSession
      .builder()
      .master(masterOfCluster)
      .appName("Join Stream With Static Data")
      .config("spark.streaming.blockInterval", "5000ms")
      .getOrCreate()

    val sparkContext = sparkSession.sparkContext
    val sqlContext = sparkSession.sqlContext

    import sqlContext.implicits._

    val customerSchema = new StructType()
      .add("cc_num", StringType, true)
      .add("first", StringType, true)
      .add("last", StringType, true)
      .add("gender", StringType, true)
      .add("street", StringType, true)
      .add("city", StringType, true)
      .add("state", StringType, true)
      .add("zip", StringType, true)
      .add("lat", DoubleType, true)
      .add("long", DoubleType, true)
      .add("job", StringType, true)
      .add("dob", TimestampType, true)


    val transactionSchema = new StructType()
      .add("cc_num", StringType, true)
      .add("trans_num", StringType, true)
      .add("trans_time", StringType, true)
      .add("category", StringType, true)
      .add("merchant", StringType, true)
      .add("amt", StringType, true)
      .add("merch_lat", StringType, true)
      .add("merch_long", StringType, true)


    val customerDF = sqlContext.read
      .option("header", "true")
      .schema(customerSchema)
      .csv(customerPath)
      .repartition('cc_num)
      .cache()

    //Action is called on customerDF so that customer DF is evalulated and cached before receiver starts receiving messages from kafka
    customerDF.count()


    val ssc = new StreamingContext(sparkContext, Seconds(10))

    val kafkaParams = Map(
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> "SingleReceiverGroup",
      "zookeeper.connection.timeout.ms" -> "1000")

    val topicMap = Map[String, Int](topic -> 1)
    val kafkaStream = KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicMap, StorageLevel.MEMORY_ONLY_SER).map(_._2)
    kafkaStream.print()

    kafkaStream.foreachRDD(rdd => {

      val transactionDF = rdd.toDF("value")
        .withColumn("transaction", from_json($"value", transactionSchema))
        .select("transaction.*")

      val joinedDF = transactionDF.join(customerDF, "cc_num")

      joinedDF.show(false)

    })

    ssc.start()
    ssc.awaitTermination()
  }
}