package com.example.chronos_node.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Configuration
public class AppConfig {

    // constructor (keeps Spring injection working too)
    public AppConfig() {}
    private String taskTopic;
    public AppConfig(String taskTopic) {
        this.taskTopic = taskTopic;
    }
    /**
     * Single source of truth for the Kafka topic name.
     * Injected from application.yml — no more hardcoded string literals in services.
     */
    @Value("${app.scheduler.topic}")
    private String taskTopic;

    public String getTaskTopic() {
        return taskTopic;
    }

    /**
     * Virtual thread executor exposed as a Spring bean so it can be injected
     * and mocked in tests instead of being created with `new` inside TaskWorker.
     */
    @Bean(destroyMethod = "shutdown")
    public ExecutorService virtualThreadExecutor() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }
}