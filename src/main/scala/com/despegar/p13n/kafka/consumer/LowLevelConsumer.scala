package com.despegar.p13n.kafka.consumer

import kafka.consumer.SimpleConsumer
import kafka.api.TopicMetadataRequest
import kafka.api.PartitionMetadata
import kafka.api.OffsetRequest
import kafka.common.TopicAndPartition
import kafka.api.PartitionOffsetRequestInfo
import kafka.common.TopicAndPartition
import kafka.api.FetchRequest
import kafka.api.FetchRequestBuilder
import kafka.common.ErrorMapping
import java.nio.charset.Charset
import java.nio.ByteBuffer

object LowLevelConsumer extends App {

  val maxReads = 10

  val consumerData = ConsumerData("testTopic", 2, List(Broker("localhost", 9092)), "simpleConsumer")
  val consumer = new LowLevelConsumer
  consumer.run(3, consumerData)
}

case class ConsumerData(topic: String, partition: Int, seedBrokers: List[Broker], groupId: String)
case class Broker(host: String, port: Int)

class LowLevelConsumer {

  def run(maxReads: Long, consumerData: ConsumerData) {
    findPartition(consumerData) match {
      case Some(partition) => runWithPartition(maxReads, partition, consumerData)
      case None            => println("Can't find metadata for Topic and Partition. Exiting")
    }
  }

  def runWithPartition(readsLimit: Long, partition: PartitionMetadata, consumerData: ConsumerData) = {
    var maxReads = readsLimit
    partition.leader match {
      case None => println("Can't find Leader for Topic and Partition. Exiting")
      case Some(leader) =>
        var leadBroker: Option[Broker] = Some(Broker(leader.host, leader.port))
        var consumer = new SimpleConsumer(leader.host, leader.port, 100000, 64 * 1024, consumerData.groupId)
        var readOffset = lastOffset(consumer, consumerData, OffsetRequest.EarliestTime)

        (1l to maxReads).foldLeft(0) { (numErrors, _) =>
          if (numErrors <= 5) {
            if (consumer == null) consumer = new SimpleConsumer(leadBroker.fold("")(_.host), leadBroker.fold(0)(_.port), 100000, 64 * 1024, consumerData.groupId)
            val fetchRequest = new FetchRequestBuilder()
              .clientId(consumerData.groupId)
              // Note: this fetchSize of 100000 might need to be increased if large batches are written to Kafka
              .addFetch(consumerData.topic, consumerData.partition, readOffset.getOrElse(0), 100000)
              .build()
            val fetchResponse = consumer.fetch(fetchRequest)
            if (fetchResponse.hasError) {
              val code = fetchResponse.errorCode(consumerData.topic, consumerData.partition)
              println(s"Error fetching data from the Broker: $leadBroker Reason: $code")
              if (code == ErrorMapping.OffsetOutOfRangeCode) {
                readOffset = lastOffset(consumer, consumerData, OffsetRequest.LatestTime)
              } else {
                consumer.close()
                consumer = null
                leadBroker = findNewLeader(leadBroker, consumerData)
              }
              numErrors + 1
            } else {
              var numRead = 0
              fetchResponse.messageSet(consumerData.topic, consumerData.partition).foreach { message =>
                var currentOffset = message.offset
                if (currentOffset < readOffset.getOrElse(0l)) {
                  println(s"Found an old offset: $currentOffset Expecting: $readOffset")
                } else {
                  readOffset = Some(message.nextOffset)
                  val payload = message.message.payload
                  val bytes = new Array[Byte](payload.remaining())
                  payload.get(bytes)
                  println(String.valueOf(message.offset) + ": " + new String(bytes, Charset.forName("UTF-8")))
                  numRead += 1
                  maxReads = maxReads - 1
                }
              }
              if (numRead == 0) {
                Thread.sleep(1000)
              }
              0
            }

          } else numErrors
        }

        if (consumer != null) consumer.close()
    }
  }

  def findPartition(consumerData: ConsumerData): Option[PartitionMetadata] = {
    val partition = consumerData.seedBrokers.foldLeft[Option[PartitionMetadata]](None) { (partition, broker) =>
      if (partition.isEmpty) {
        var consumer: SimpleConsumer = null
        try {
          consumer = new SimpleConsumer(broker.host, broker.port, 100000, 64 * 1024, consumerData.groupId)
          val request: TopicMetadataRequest = new TopicMetadataRequest(List(consumerData.topic), 0)
          val response = consumer.send(request)

          val topic = response.topicsMetadata.find { topicMetadata =>
            topicMetadata.topic.equalsIgnoreCase(consumerData.topic)
          }

          topic.flatMap { topicMetadata =>
            topicMetadata.partitionsMetadata.find { partition =>
              partition.partitionId == consumerData.partition
            }
          }
        } catch {
          case e: Throwable =>
            println(s"Error communicating with broker ")
            None
        } finally {
          if (consumer != null) consumer.close
        }
      } else {
        partition
      }
    }
    partition
  }

  def findNewLeader(oldLeader: Option[Broker], consumerData: ConsumerData): Option[Broker] = {
    (1 to 3).foldLeft[Option[Broker]](None) { (leader, count) =>
      if (leader.isEmpty) {
        val newLeader = for {
          partition <- findPartition(consumerData)
          broker <- partition.leader if Broker(broker.host, broker.port) != oldLeader.getOrElse(Broker("", 0)) || count != 0
        } yield Broker(broker.host, broker.port)

        if (newLeader.isEmpty) {
          Thread.sleep(1000)
        }
        newLeader
      } else {
        leader
      }
    }
  }

  def lastOffset(consumer: SimpleConsumer, consumerData: ConsumerData, wichTime: Long): Option[Long] = {
    val topicAndPartition = TopicAndPartition(consumerData.topic, consumerData.partition)
    val partitionOffset = PartitionOffsetRequestInfo(wichTime, 1)
    val requestInfo = Map(topicAndPartition -> partitionOffset)
    val request: OffsetRequest = OffsetRequest(requestInfo)
    val response = consumer.getOffsetsBefore(request)

    if (!response.hasError) {
      for {
        offsets <- response.offsetsGroupedByTopic.get(consumerData.topic)
        partitionResponse <- offsets.get(TopicAndPartition(consumerData.topic, consumerData.partition))
      } yield partitionResponse.offsets.headOption.getOrElse(0)
    } else {
      println(s"Error fetching data Offset Data the Broker. Reason: ${response.partitionErrorAndOffsets}")
      None
    }
  }

}