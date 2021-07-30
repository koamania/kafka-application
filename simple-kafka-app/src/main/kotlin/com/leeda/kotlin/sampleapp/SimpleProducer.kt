package com.leeda.kotlin.sampleapp

import org.apache.kafka.clients.producer.*
import org.apache.kafka.common.record.CompressionType
import org.apache.kafka.common.serialization.StringSerializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.lang.Exception
import java.util.*
import kotlin.reflect.jvm.jvmName

class SimpleProducer {

    private val logger: Logger get() = LoggerFactory.getLogger(this.javaClass)

    fun test() {
        val configs = Properties()
        configs[ProducerConfig.BOOTSTRAP_SERVERS_CONFIG] = SimpleKafkaConst.BOOTSTRAP_SERVERS
        configs[ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.jvmName
        configs[ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG] = StringSerializer::class.jvmName
        configs[ProducerConfig.COMPRESSION_TYPE_CONFIG] = CompressionType.GZIP.name
        configs[ProducerConfig.PARTITIONER_CLASS_CONFIG] = CustomPartitioner::class.jvmName

        val producer = KafkaProducer<String, String>(configs)
        val callback = { recordMetadata: RecordMetadata?, e: Exception? ->
            e?.let {
                logger.error("message send fail!!", it)
            } ?: logger.info("message send success : $recordMetadata")
        }

        val messageValue = "testMessage"

        val record = ProducerRecord(SimpleKafkaConst.TOPIC_NAME, "test-key", messageValue)
        producer.send(record, callback)

        val record2 = ProducerRecord<String, String>(SimpleKafkaConst.TOPIC_NAME, messageValue)

        producer.send(record2, callback)

        producer.flush()
        producer.close()
    }
}

fun main(args: Array<String>) {
    SimpleProducer().test()
}