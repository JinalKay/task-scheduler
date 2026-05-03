package com.example.chronos_node.config;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

@Configuration
public class AppConfig {

    @Value("${app.scheduler.topic}")
    private String taskTopic;

    // No-arg constructor for Spring
    public AppConfig() {}

    // Constructor for tests — lets tests create a real instance without Spring context
    public AppConfig(String taskTopic) {
        this.taskTopic = taskTopic;
    }

    public String getTaskTopic() {
        return taskTopic;
    }

    /**
     * Virtual thread executor as a Spring bean — injected into TaskWorker,
     * mockable/replaceable in tests.
     */
    @Bean(destroyMethod = "shutdown")
    public ExecutorService virtualThreadExecutor() {
        return Executors.newVirtualThreadPerTaskExecutor();
    }
}