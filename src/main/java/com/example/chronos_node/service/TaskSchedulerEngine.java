package com.example.chronos_node.service;

import com.example.chronos_node.model.Task;
import com.example.chronos_node.model.TaskStatus;
import com.example.chronos_node.repository.TaskRepository;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.concurrent.TimeUnit;

@Service
public class TaskSchedulerEngine {

    private static final Logger log = LoggerFactory.getLogger(TaskSchedulerEngine.class); // Manual Logger

    private final TaskRepository taskRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final RedissonClient redisson;

    public TaskSchedulerEngine(TaskRepository taskRepository, KafkaTemplate<String, String> kafkaTemplate, RedissonClient redisson) {
        this.taskRepository = taskRepository;
        this.kafkaTemplate = kafkaTemplate;
        this.redisson = redisson;
    }

    @Scheduled(fixedDelay = 10000)
    public void fetchAndDispatchTasks() {
        RLock lock = redisson.getLock("scheduler-leader-lock");
        try {
            if (lock.tryLock(0, 5, TimeUnit.SECONDS)) {
                log.info("I am the Leader. Scanning for tasks...");
                processPendingTasks();
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } finally {
            if (lock.isHeldByCurrentThread()) lock.unlock();
        }
    }

    @Transactional
    public void processPendingTasks() {
        List<Task> tasks = taskRepository.findReadyTasks(LocalDateTime.now());
        for (Task task : tasks) {
            task.setStatus(TaskStatus.QUEUED);
            taskRepository.save(task);
            kafkaTemplate.send("task-topic", task.getId().toString());
            log.info("Task {} dispatched to Kafka", task.getName());
        }
    }
}