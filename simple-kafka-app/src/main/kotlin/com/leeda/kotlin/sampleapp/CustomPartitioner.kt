package com.leeda.kotlin.sampleapp

import org.apache.kafka.clients.producer.Partitioner
import org.apache.kafka.common.Cluster
import org.apache.kafka.common.InvalidRecordException
import org.apache.kafka.common.PartitionInfo
import org.apache.kafka.common.utils.Utils

class CustomPartitioner: Partitioner {
    override fun partition(
        topic: String,
        key: Any?,
        keyBytes: ByteArray?,
        value: Any,
        valueBytes: ByteArray,
        cluster: Cluster
    ): Int {
        keyBytes?.let {
            if (key as String == "test-key") {
                return 0
            }

            val partitions: List<PartitionInfo> = cluster.partitionsForTopic(topic)
            val numPartitions = partitions.size

            return Utils.toPositive(Utils.murmur2(keyBytes)) % numPartitions
        } ?: throw InvalidRecordException("Need message key")
    }

    override fun configure(configs: MutableMap<String, *>?) {

    }

    override fun close() {

    }
}