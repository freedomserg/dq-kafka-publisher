package dataquality.kafka.generator

import java.util.Properties

import org.apache.kafka.clients.producer.{KafkaProducer, ProducerConfig, ProducerRecord}

import scala.io.{BufferedSource, Source}

object KafkaTransactionPublisher {

  def main(args: Array[String]) {
    val bootstrapServers = args.headOption.getOrElse("localhost:9092")
    val topic = args.lift(1).getOrElse("transactions")
    val file = args.lift(2).getOrElse("transactions")
    val recordsPartitionSize = args.lift(3).map(_.toInt).getOrElse(100)
    val delayUntilNextSend = args.lift(4).map(_.toInt * 1000).getOrElse(5000)

    val props = new Properties()
    props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers)
    props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")
    props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer")

    val producer = new KafkaProducer[Nothing, String](props)

    var fileSource: BufferedSource = null

    try {
      println(s"Begin reading transactions from file source '$file' and publishing to '$topic' Kafka topic'")
      while (true) {
        fileSource = Source.fromFile(file)
        var currentStep = 0
        println(s"Waiting ${delayUntilNextSend / 1000} seconds...")
        Thread.sleep(delayUntilNextSend)
        for (txLine <- fileSource.getLines) {
          if (currentStep >= recordsPartitionSize) {
            currentStep = 0
            // wait
            println(s"Waiting ${delayUntilNextSend / 1000} seconds...")
            Thread.sleep(delayUntilNextSend)
          }

          producer.send(new ProducerRecord(topic, txLine))
          println("Sent -> " + txLine)
          currentStep += 1
        }
        fileSource.close()
      }
    } finally {
      if (fileSource != null) {
        fileSource.close()
      }
      producer.close()
    }
  }
}

