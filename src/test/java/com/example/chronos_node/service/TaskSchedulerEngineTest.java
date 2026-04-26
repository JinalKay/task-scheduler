package com.example.chronos_node.service;

import com.example.chronos_node.config.AppConfig;
import com.example.chronos_node.model.Task;
import com.example.chronos_node.model.TaskStatus;
import com.example.chronos_node.repository.TaskRepository;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;
import org.springframework.kafka.core.KafkaTemplate;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TaskSchedulerEngineTest {

    @Mock private TaskRepository taskRepository;
    @Mock private KafkaTemplate<String, String> kafkaTemplate;
    @Mock private RedissonClient redisson;
    private final AppConfig appConfig = new AppConfig("task-topic");
    @Mock private RLock rLock;

    @InjectMocks
    private TaskSchedulerEngine engine;

    @BeforeEach
    void setUp() {
        when(redisson.getLock(anyString())).thenReturn(rLock);
        when(appConfig.getTaskTopic()).thenReturn("task-topic");
    }

    @Test
    @DisplayName("dispatches ready tasks: status → QUEUED and sends to Kafka")
    void processPendingTasks_dispatchesReadyTasks() {
        Task t1 = pendingTask("TaskA");
        Task t2 = pendingTask("TaskB");
        when(taskRepository.findReadyTasks(eq(TaskStatus.PENDING), any(LocalDateTime.class)))
            .thenReturn(List.of(t1, t2));

        engine.processPendingTasks();

        ArgumentCaptor<Task> saved = ArgumentCaptor.forClass(Task.class);
        verify(taskRepository, times(2)).save(saved.capture());
        assertThat(saved.getAllValues()).allMatch(t -> t.getStatus() == TaskStatus.QUEUED);
        verify(kafkaTemplate).send("task-topic", t1.getId().toString());
        verify(kafkaTemplate).send("task-topic", t2.getId().toString());
    }

    @Test
    @DisplayName("does nothing when no tasks are ready")
    void processPendingTasks_noReadyTasks_doesNothing() {
        when(taskRepository.findReadyTasks(any(), any())).thenReturn(List.of());

        engine.processPendingTasks();

        verify(taskRepository, never()).save(any());
        verifyNoInteractions(kafkaTemplate);
    }

    @Test
    @DisplayName("runs processPendingTasks only when Redis lock is acquired")
    void fetchAndDispatchTasks_runsWhenLockAcquired() throws InterruptedException {
        when(rLock.tryLock(0, 5, TimeUnit.SECONDS)).thenReturn(true);
        when(rLock.isHeldByCurrentThread()).thenReturn(true);
        when(taskRepository.findReadyTasks(any(), any())).thenReturn(List.of());

        engine.fetchAndDispatchTasks();

        verify(taskRepository).findReadyTasks(any(), any());
        verify(rLock).unlock();
    }

    @Test
    @DisplayName("skips processing when another node holds the leader lock")
    void fetchAndDispatchTasks_skipsWhenLockNotAcquired() throws InterruptedException {
        when(rLock.tryLock(0, 5, TimeUnit.SECONDS)).thenReturn(false);

        engine.fetchAndDispatchTasks();

        verifyNoInteractions(taskRepository);
        verifyNoInteractions(kafkaTemplate);
    }

    @Test
    @DisplayName("always releases the lock even if processing throws")
    void fetchAndDispatchTasks_releasesLockOnException() throws InterruptedException {
        when(rLock.tryLock(0, 5, TimeUnit.SECONDS)).thenReturn(true);
        when(rLock.isHeldByCurrentThread()).thenReturn(true);
        when(taskRepository.findReadyTasks(any(), any())).thenThrow(new RuntimeException("DB down"));

        try { engine.fetchAndDispatchTasks(); } catch (Exception ignored) {}

        verify(rLock).unlock();
    }

    private Task pendingTask(String name) {
        Task t = new Task();
        t.setId(UUID.randomUUID());
        t.setName(name);
        t.setStatus(TaskStatus.PENDING);
        t.setScheduledAt(LocalDateTime.now().minusMinutes(1));
        return t;
    }
}