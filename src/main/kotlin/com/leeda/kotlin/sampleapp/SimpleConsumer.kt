package com.leeda.kotlin.sampleapp

import org.apache.kafka.clients.consumer.ConsumerConfig.*
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import kotlin.reflect.jvm.jvmName

class SimpleConsumer {
    private val logger: Logger get() = LoggerFactory.getLogger(this.javaClass)

    companion object {
        const val GROUP_ID = "test-group"
    }

    fun test() {
        val configs = mapOf(
            BOOTSTRAP_SERVERS_CONFIG to SimpleKafkaConst.BOOTSTRAP_SERVERS,
            GROUP_ID_CONFIG to GROUP_ID,
            KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.jvmName,
            VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.jvmName
        ).toProperties()

        val consumer = KafkaConsumer<String, String>(configs)
        consumer.subscribe(listOf(SimpleKafkaConst.TOPIC_NAME))
        consume(consumer)
    }

    fun consume(consumer: KafkaConsumer<String, String>) {
        while(true) {
            val records = consumer.poll(Duration.ofSeconds(1))
            records.forEach {
                logger.info("$it")
            }
        }
    }
}

fun main(args: Array<String>) {
    SimpleConsumer().test()
}