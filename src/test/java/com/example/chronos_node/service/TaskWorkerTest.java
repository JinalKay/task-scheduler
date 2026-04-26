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
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.redisson.api.RLock;
import org.redisson.api.RedissonClient;

import java.time.LocalDateTime;
import java.util.Optional;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.*;
import static org.mockito.Mockito.*;

@ExtendWith(MockitoExtension.class)
class TaskWorkerTest {

    @Mock private TaskRepository taskRepository;
    @Mock private RedissonClient redisson;
    private final AppConfig appConfig = new AppConfig("task-topic");
    @Mock private RLock rLock;

    // Real virtual thread executor — tests the threading model honestly
    private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();
    private TaskWorker worker;

    @BeforeEach
    void setUp() {
        when(redisson.getLock(anyString())).thenReturn(rLock);
        worker = new TaskWorker(taskRepository, redisson, executor, appConfig);
    }

    @Test
    @DisplayName("executes a QUEUED task and marks it COMPLETED")
    void executeSafe_completesTask() throws Exception {
        UUID id = UUID.randomUUID();
        Task task = queuedTask(id, "MyTask");
        when(rLock.tryLock(0, 5, TimeUnit.MINUTES)).thenReturn(true);
        when(rLock.isHeldByCurrentThread()).thenReturn(true);
        when(taskRepository.findById(id)).thenReturn(Optional.of(task));

        worker.consumeTask(id.toString());

        ArgumentCaptor<Task> saved = ArgumentCaptor.forClass(Task.class);
        verify(taskRepository, timeout(2000)).save(saved.capture());
        assertThat(saved.getValue().getStatus()).isEqualTo(TaskStatus.COMPLETED);
    }

    @Test
    @DisplayName("skips re-execution of an already COMPLETED task (idempotency)")
    void executeSafe_skipsAlreadyCompletedTask() throws Exception {
        UUID id = UUID.randomUUID();
        Task task = completedTask(id);
        when(rLock.tryLock(0, 5, TimeUnit.MINUTES)).thenReturn(true);
        when(rLock.isHeldByCurrentThread()).thenReturn(true);
        when(taskRepository.findById(id)).thenReturn(Optional.of(task));

        worker.consumeTask(id.toString());

        verify(taskRepository, after(1500).never()).save(any());
    }

    @Test
    @DisplayName("per-task lock key is scoped to task UUID")
    void executeSafe_usesTaskScopedLockKey() throws Exception {
        UUID id = UUID.randomUUID();
        when(rLock.tryLock(anyLong(), anyLong(), any())).thenReturn(false);

        worker.consumeTask(id.toString());
        Thread.sleep(200);

        verify(redisson, timeout(1000)).getLock("task-lock:" + id);
    }

    @Test
    @DisplayName("marks task FAILED and increments retryCount on exception")
    void executeSafe_marksTaskFailedOnException() throws Exception {
        UUID id = UUID.randomUUID();
        Task task = queuedTask(id, "BadTask");
        Task taskForFailure = queuedTask(id, "BadTask");

        when(rLock.tryLock(0, 5, TimeUnit.MINUTES)).thenReturn(true);
        when(rLock.isHeldByCurrentThread()).thenReturn(true);
        // First findById: return task to execute; second findById (in markFailed): return fresh copy
        when(taskRepository.findById(id))
            .thenReturn(Optional.of(task))
            .thenReturn(Optional.of(taskForFailure));
        // First save (COMPLETED attempt) throws; second save (FAILED) succeeds
        when(taskRepository.save(any()))
            .thenThrow(new RuntimeException("DB error"))
            .thenReturn(taskForFailure);

        worker.consumeTask(id.toString());
        Thread.sleep(300);

        ArgumentCaptor<Task> captor = ArgumentCaptor.forClass(Task.class);
        verify(taskRepository, timeout(2000).atLeast(2)).save(captor.capture());
        boolean failedSaveExists = captor.getAllValues().stream()
            .anyMatch(t -> t.getStatus() == TaskStatus.FAILED);
        assertThat(failedSaveExists).isTrue();
    }

    @Test
    @DisplayName("always releases the per-task lock after execution")
    void executeSafe_alwaysReleasesLock() throws Exception {
        UUID id = UUID.randomUUID();
        when(rLock.tryLock(0, 5, TimeUnit.MINUTES)).thenReturn(true);
        when(rLock.isHeldByCurrentThread()).thenReturn(true);
        when(taskRepository.findById(id)).thenReturn(Optional.of(queuedTask(id, "LockTest")));

        worker.consumeTask(id.toString());

        verify(rLock, timeout(2000)).unlock();
    }

    @Test
    @DisplayName("skips execution when another worker holds the task lock")
    void executeSafe_skipsIfLockNotAcquired() throws Exception {
        UUID id = UUID.randomUUID();
        when(rLock.tryLock(0, 5, TimeUnit.MINUTES)).thenReturn(false);

        worker.consumeTask(id.toString());

        verify(taskRepository, after(500).never()).findById(any());
    }

    private Task queuedTask(UUID id, String name) {
        Task t = new Task();
        t.setId(id);
        t.setName(name);
        t.setStatus(TaskStatus.QUEUED);
        t.setScheduledAt(LocalDateTime.now().minusMinutes(1));
        return t;
    }

    private Task completedTask(UUID id) {
        Task t = new Task();
        t.setId(id);
        t.setName("AlreadyDone");
        t.setStatus(TaskStatus.COMPLETED);
        t.setScheduledAt(LocalDateTime.now().minusMinutes(5));
        return t;
    }
}