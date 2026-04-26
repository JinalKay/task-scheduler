package com.example.chronos_node.service;

import com.example.chronos_node.config.AppConfig;
import com.example.chronos_node.model.Task;
import com.example.chronos_node.model.TaskStatus;
import com.example.chronos_node.repository.TaskRepository;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.annotation.RetryableTopic;
import org.springframework.retry.annotation.Backoff;
import org.springframework.stereotype.Service;

import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

@Service
public class TaskWorker {

    private static final Logger log = LoggerFactory.getLogger(TaskWorker.class);

    private final TaskRepository taskRepository;
    private final RedissonClient redisson;
    private final ExecutorService executor;    // Injected bean — testable, lifecycle managed
    private final AppConfig appConfig;

    public TaskWorker(TaskRepository taskRepository,
                      RedissonClient redisson,
                      ExecutorService executor,
                      AppConfig appConfig) {
        this.taskRepository = taskRepository;
        this.redisson       = redisson;
        this.executor       = executor;
        this.appConfig      = appConfig;
    }

    /**
     * Kafka listener — receives task IDs from the scheduler.
     * Immediately hands off to a virtual thread so the Kafka consumer thread
     * is never blocked and can keep polling new messages.
     *
     * @RetryableTopic: 3 attempts with 1-second exponential backoff before
     * routing to the Dead Letter Topic (task-topic-dlt).
     */
    @RetryableTopic(attempts = "3", backoff = @Backoff(delay = 1000, multiplier = 2.0))
    @KafkaListener(topics = "${app.scheduler.topic}", groupId = "chronos-group")
    public void consumeTask(String taskId) {
        executor.submit(() -> executeSafe(UUID.fromString(taskId)));
    }

    private void executeSafe(UUID taskId) {
        String lockKey = "task-lock:" + taskId;
        RLock lock = redisson.getLock(lockKey);
        try {
            if (!lock.tryLock(0, 5, TimeUnit.MINUTES)) {
                log.debug("Task {} is already being processed by another worker — skipping", taskId);
                return;
            }

            Task task = taskRepository.findById(taskId)
                .orElseThrow(() -> new IllegalStateException("Task not found in DB: " + taskId));

            // Idempotency guard: if already completed (duplicate message), do nothing
            if (task.getStatus() == TaskStatus.COMPLETED) {
                log.debug("Task {} is already COMPLETED — skipping duplicate execution", taskId);
                return;
            }

            log.info("Executing task '{}' (id={}) on {}", task.getName(), taskId, Thread.currentThread());

            // === Replace this block with actual business logic ===
            Thread.sleep(1000); // Simulates I/O-bound work (e.g. HTTP call, DB write)
            // =====================================================

            task.setStatus(TaskStatus.COMPLETED);
            taskRepository.save(task);
            log.info("Task '{}' completed successfully", task.getName());

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Task {} execution interrupted", taskId);
            markFailed(taskId);

        } catch (Exception e) {
            log.error("Task {} failed with exception: {}", taskId, e.getMessage(), e);
            markFailed(taskId);

        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    /**
     * Best-effort persistence of the FAILED status.
     * Increments retryCount so the system knows how many times this task has failed.
     */
    private void markFailed(UUID taskId) {
        try {
            taskRepository.findById(taskId).ifPresent(t -> {
                t.setStatus(TaskStatus.FAILED);
                t.setRetryCount(t.getRetryCount() + 1);
                taskRepository.save(t);
                log.info("Task {} marked as FAILED (retryCount={})", taskId, t.getRetryCount());
            });
        } catch (Exception saveEx) {
            log.error("Could not persist FAILED status for task {} — DB may be unavailable", taskId, saveEx);
        }
    }
}