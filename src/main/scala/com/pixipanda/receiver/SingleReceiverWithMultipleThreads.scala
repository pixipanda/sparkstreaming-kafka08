package  com.pixipanda.receiver

import kafka.serializer.StringDecoder
import org.apache.spark.SparkConf
import org.apache.spark.storage.StorageLevel
import org.apache.spark.streaming.kafka.KafkaUtils
import org.apache.spark.streaming.{Seconds, StreamingContext}


object SingleReceiverWithMultipleThreads {

  def main(args: Array[String]) {


    val masterOfCluster = args(0)
    val topic = args(1)

    val conf = new SparkConf()
      .setMaster(masterOfCluster)
      .setAppName("NetworkWordCount")
      .set("spark.streaming.blockInterval", "1000ms")
    val ssc = new StreamingContext(conf, Seconds(5))


    val kafkaParams = Map(
      "zookeeper.connect" -> "localhost:2181",
      "group.id" -> "SingleReceiverGroup",
      "zookeeper.connection.timeout.ms" -> "1000")


    val consumerThreadsPerInputDstream = 3

    val topicMap = Map[String, Int](topic -> consumerThreadsPerInputDstream)
    val kafkaStream =  KafkaUtils.createStream[String, String, StringDecoder, StringDecoder](ssc, kafkaParams, topicMap, StorageLevel.MEMORY_ONLY_SER).map(_._2)
    kafkaStream.print()

    ssc.start()
    ssc.awaitTermination()
  }
}