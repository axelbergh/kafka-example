package com.despegar.p13n.kafka.client

import kafka.consumer.SimpleConsumer
import kafka.api.TopicMetadataRequest
import kafka.api.PartitionMetadata
import kafka.api.OffsetRequest
import kafka.common.TopicAndPartition
import kafka.api.PartitionOffsetRequestInfo
import kafka.common.TopicAndPartition

object LowLevelConsumer extends App {

  val maxReads = 10

  val consumerData = ConsumerData("testTopic", 0, List(Broker("localhost", 9092)), "simpleConsumer")
  val consumer = new LowLevelConsumer
  consumer.run(10, consumerData)
}

case class ConsumerData(topic: String, partition: Int, seedBrokers: List[Broker], groupId: String)
case class Broker(host: String, port: Int)

class LowLevelConsumer {

  def run(maxReads: Long, consumerData: ConsumerData) {
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
    
    if(!response.hasError) {
      for{ offsets <- response.offsetsGroupedByTopic.get(consumerData.topic)
        partitionResponse <- offsets.get(TopicAndPartition(consumerData.topic, consumerData.partition))
      } yield partitionResponse.offsets.headOption.getOrElse(0)
    } else {
      println(s"Error fetching data Offset Data the Broker. Reason: ${response.partitionErrorAndOffsets}")
      None
    }
  }

}