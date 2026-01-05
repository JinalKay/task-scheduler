package com.example.chronos_node.service;

import com.example.chronos_node.model.Task;
import com.example.chronos_node.model.TaskStatus;
import com.example.chronos_node.repository.TaskRepository;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.kafka.retrytopic.DltStrategy;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

@Service
public class TaskWorker {

    private static final Logger log = LoggerFactory.getLogger(TaskWorker.class); // Manual Logger

    private final TaskRepository taskRepository;
    private final RedissonClient redisson;
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

    public TaskWorker(TaskRepository taskRepository, RedissonClient redisson) {
        this.taskRepository = taskRepository;
        this.redisson = redisson;
    }

    @RetryableTopic(attempts = "3", backoff = @Backoff(delay = 1000))
    @KafkaListener(topics = "task-topic", groupId = "chronos-group")
    public void consumeTask(String taskId) {
        executor.submit(() -> executeSafe(UUID.fromString(taskId)));
    }

    private void executeSafe(UUID taskId) {
        String lockKey = "task-lock:" + taskId;
        RLock lock = redisson.getLock(lockKey);
        try {
            if (lock.tryLock(0, 5, TimeUnit.MINUTES)) {
                Task task = taskRepository.findById(taskId).orElseThrow();
                if (task.getStatus() == TaskStatus.COMPLETED) return;

                log.info("Executing {} on Virtual Thread: {}", task.getName(), Thread.currentThread());
                Thread.sleep(1000); 
                
                task.setStatus(TaskStatus.COMPLETED);
                taskRepository.save(task);
            }
        } catch (Exception e) {
            log.error("Task failed", e);
        } finally {
            if (lock.isHeldByCurrentThread()) lock.unlock();
        }
    }
}