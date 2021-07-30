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
        // 키는 토픽과 파티션 정보가 담긴 ToPicPartition가 되고 값은 오프셋 정보가 담긴 OffsetAndMetadata가 된다.
        val currentOffset = mutableMapOf<TopicPartition, OffsetAndMetadata>()

        while (true) {
            val records = consumer.poll(Duration.ofSeconds(1))
            records.forEach {
                logger.info("record : $it")
                // 처리를 완료한 레코드의 정보를 토대로 키/값을 넣는다.
                // 이때 주의할 점은 현재 처리한 오프셋에 1을 더한 값을 커밋해야 한다는 점
                // consumer가 poll()을 수행할 때 마지막으로 커밋한 오프셋부터 레코드를 리턴하기 떄문이다
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
        // shutdown hook으로 wakeup을 호출해 안전하게 종료
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