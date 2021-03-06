package com.leeda.kotlin.sampleapp

import org.apache.kafka.clients.consumer.ConsumerConfig.*
import org.apache.kafka.clients.consumer.ConsumerRebalanceListener
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.clients.consumer.OffsetAndMetadata
import org.apache.kafka.common.TopicPartition
import org.apache.kafka.common.errors.WakeupException
import org.apache.kafka.common.serialization.StringDeserializer
import org.slf4j.Logger
import org.slf4j.LoggerFactory
import java.time.Duration
import kotlin.concurrent.thread
import kotlin.reflect.jvm.jvmName

class SimpleConsumer {
    private val logger: Logger get() = LoggerFactory.getLogger(this.javaClass)

    companion object {
        const val GROUP_ID = "test-group"
        val configs = mapOf(
            BOOTSTRAP_SERVERS_CONFIG to SimpleKafkaConst.BOOTSTRAP_SERVERS,
            GROUP_ID_CONFIG to GROUP_ID,
            KEY_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.jvmName,
            VALUE_DESERIALIZER_CLASS_CONFIG to StringDeserializer::class.jvmName
        ).toProperties()
    }

    fun consumerTest() {
        val consumer = KafkaConsumer<String, String>(configs)
        consumer.subscribe(listOf(SimpleKafkaConst.TOPIC_NAME))
        while(true) {
            val records = consumer.poll(Duration.ofSeconds(1))
            records.forEach {
                logger.info("$it")
            }
        }

    }

    fun commitSyncTest() {
        configs[ENABLE_AUTO_COMMIT_CONFIG] = false

        val consumer = KafkaConsumer<String, String>(configs)
        consumer.subscribe(listOf(SimpleKafkaConst.TOPIC_NAME))

        while (true) {
            val records = consumer.poll(Duration.ofSeconds(1))
            records.forEach {
                logger.info("record : $it")
            }

            consumer.commitSync()
        }
    }

    fun commitSyncWithOffsetTest() {
        configs[ENABLE_AUTO_COMMIT_CONFIG] = false

        val consumer = KafkaConsumer<String, String>(configs)
        consumer.subscribe(listOf(SimpleKafkaConst.TOPIC_NAME))
        // ?????? ????????? ????????? ????????? ?????? ToPicPartition??? ?????? ?????? ????????? ????????? ?????? OffsetAndMetadata??? ??????.
        val currentOffset = mutableMapOf<TopicPartition, OffsetAndMetadata>()

        while (true) {
            val records = consumer.poll(Duration.ofSeconds(1))
            records.forEach {
                logger.info("record : $it")
                // ????????? ????????? ???????????? ????????? ????????? ???/?????? ?????????.
                // ?????? ????????? ?????? ?????? ????????? ???????????? 1??? ?????? ?????? ???????????? ????????? ???
                // consumer??? poll()??? ????????? ??? ??????????????? ????????? ??????????????? ???????????? ???????????? ????????????
                currentOffset[TopicPartition(it.topic(), it.partition())] = OffsetAndMetadata(it.offset() + 1, null)
                consumer.commitSync(currentOffset)
            }
        }
    }

    fun commitAsyncTest() {
        configs[ENABLE_AUTO_COMMIT_CONFIG] = false

        val consumer = KafkaConsumer<String, String>(configs)
        consumer.subscribe(listOf(SimpleKafkaConst.TOPIC_NAME))

        while (true) {
            logger.info("poll message")
            val records = consumer.poll(Duration.ofSeconds(1))
            records.forEach {
                logger.info("record : $it")
            }

            consumer.commitAsync { _, _ ->
                logger.info("commit success")
            }
        }
    }

    fun consumerWithWakeupTest() {
        val consumer = KafkaConsumer<String, String>(configs)
        // shutdown hook?????? wakeup??? ????????? ???????????? ??????
        Runtime.getRuntime().addShutdownHook(thread {
            consumer.wakeup()
        })
        consumer.subscribe(listOf(SimpleKafkaConst.TOPIC_NAME))

        try {
            while(true) {
                val records = consumer.poll(Duration.ofSeconds(1))
                records.forEach {
                    logger.info("$it")
                }
            }
        } catch (e: WakeupException) {
            logger.warn("Wakeup consumer")
        } finally {
            consumer.close()
        }
    }

}

class RebalanceListener: ConsumerRebalanceListener {

    private val logger: Logger get() = LoggerFactory.getLogger(this.javaClass)

    override fun onPartitionsRevoked(partitions: MutableCollection<TopicPartition>) {
        logger.warn("Partitions are assigned")
    }

    override fun onPartitionsAssigned(partitions: MutableCollection<TopicPartition>) {
        logger.warn("Partitions are revoked")
//        consumer.commitSync(currentOffsets)
    }
}


fun main(args: Array<String>) {
    SimpleConsumer().commitAsyncTest()
}