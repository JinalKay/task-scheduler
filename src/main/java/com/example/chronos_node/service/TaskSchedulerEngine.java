package com.example.chronos_node.service;

import com.example.chronos_node.config.AppConfig;
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

    private static final Logger log = LoggerFactory.getLogger(TaskSchedulerEngine.class);
    private static final String LEADER_LOCK_KEY = "scheduler-leader-lock";

    private final TaskRepository taskRepository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final RedissonClient redisson;
    private final AppConfig appConfig;

    public TaskSchedulerEngine(TaskRepository taskRepository,
                               KafkaTemplate<String, String> kafkaTemplate,
                               RedissonClient redisson,
                               AppConfig appConfig) {
        this.taskRepository = taskRepository;
        this.kafkaTemplate  = kafkaTemplate;
        this.redisson       = redisson;
        this.appConfig      = appConfig;
    }

    /**
     * Runs every 10 seconds. Attempts leader-election via Redis lock.
     * Only the node that wins the lock dispatches tasks — all others skip.
     */
    @Scheduled(fixedDelayString = "${app.scheduler.poll-delay-ms:10000}")
    public void fetchAndDispatchTasks() {
        RLock lock = redisson.getLock(LEADER_LOCK_KEY);
        try {
            if (lock.tryLock(0, 5, TimeUnit.SECONDS)) {
                log.info("Leader elected — scanning for ready tasks");
                processPendingTasks();
            } else {
                log.debug("Lock not acquired — another node is the leader this cycle");
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            log.warn("Scheduler interrupted during leader election");
        } finally {
            if (lock.isHeldByCurrentThread()) {
                lock.unlock();
            }
        }
    }

    /**
     * Fetches all PENDING tasks whose scheduled time has passed, marks them QUEUED,
     * and publishes their IDs to Kafka. The @Version on Task provides optimistic locking —
     * if two nodes somehow both get here, only one wins; the other gets an
     * OptimisticLockException which propagates up and leaves the task PENDING for the next cycle.
     */
    @Transactional
    public void processPendingTasks() {
        List<Task> tasks = taskRepository.findReadyTasks(TaskStatus.PENDING, LocalDateTime.now());
        if (tasks.isEmpty()) {
            log.debug("No tasks ready for dispatch");
            return;
        }
        log.info("Dispatching {} task(s) to Kafka topic '{}'", tasks.size(), appConfig.getTaskTopic());
        for (Task task : tasks) {
            task.setStatus(TaskStatus.QUEUED);
            taskRepository.save(task);
            kafkaTemplate.send(appConfig.getTaskTopic(), task.getId().toString());
            log.debug("Task '{}' ({}) dispatched", task.getName(), task.getId());
        }
    }
}