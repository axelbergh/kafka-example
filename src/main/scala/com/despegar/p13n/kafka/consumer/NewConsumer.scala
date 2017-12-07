package com.despegar.p13n.kafka.consumer

import java.util.Properties

import org.apache.kafka.clients.consumer.ConsumerRecords
import org.apache.kafka.clients.consumer.KafkaConsumer
import collection.JavaConverters._

object NewConsumer extends App {
  val props: Properties = new Properties()
  //  props.put("bootstrap.servers", "localhost:9092,localhost:9093,localhost:9094")
  props.put("bootstrap.servers", "p13n-kafka-rc-01.servers.despegar.it:9092")
  props.put("group.id", "newConsumer")
    props.put("auto.offset.reset", "earliest")
  props.put("enable.auto.commit", "true")
  props.put("auto.commit.interval.ms", "1000")
  props.put("session.timeout.ms", "30000")
  props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer")
  val consumer: KafkaConsumer[String, String] = new KafkaConsumer(props)
    consumer.subscribe(List("kafka-example").asJava)
//  consumer.subscribe(List("prometeo-jean-retries").asJava)
  while (true) {
    val records: ConsumerRecords[String, String] = consumer.poll(1)
    for (record <- records.asScala) {
      println(s"offset = ${record.offset()}, key = ${record.key()}, value = ${record.value()}, partition = ${record.partition()}")
    }
  }
}