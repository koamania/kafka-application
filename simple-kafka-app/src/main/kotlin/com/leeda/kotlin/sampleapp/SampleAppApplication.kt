package com.leeda.kotlin.sampleapp

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication

@SpringBootApplication
class SampleAppApplication

fun main(args: Array<String>) {
    runApplication<SampleAppApplication>(*args)
}
