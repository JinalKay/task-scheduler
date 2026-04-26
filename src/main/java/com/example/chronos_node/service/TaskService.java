package com.example.chronos_node.service;

import com.example.chronos_node.dto.CreateTaskRequest;
import com.example.chronos_node.dto.TaskResponse;
import com.example.chronos_node.model.Task;
import com.example.chronos_node.model.TaskStatus;
import com.example.chronos_node.repository.TaskRepository;
import jakarta.persistence.EntityNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Service
public class TaskService {

    private static final Logger log = LoggerFactory.getLogger(TaskService.class);
    private final TaskRepository taskRepository;

    public TaskService(TaskRepository taskRepository) {
        this.taskRepository = taskRepository;
    }

    @Transactional
    public TaskResponse createTask(CreateTaskRequest request) {
        Task task = new Task();
        task.setName(request.getName());
        task.setStatus(TaskStatus.PENDING);
        task.setScheduledAt(
            request.getScheduledAt() != null ? request.getScheduledAt() : LocalDateTime.now()
        );
        Task saved = taskRepository.save(task);
        log.debug("Created task '{}' id={}", saved.getName(), saved.getId());
        return TaskResponse.from(saved);
    }

    @Transactional(readOnly = true)
    public TaskResponse getTask(UUID id) {
        Task task = taskRepository.findById(id)
            .orElseThrow(() -> new EntityNotFoundException("Task not found: " + id));
        return TaskResponse.from(task);
    }

    @Transactional(readOnly = true)
    public List<TaskResponse> getTasksByStatus(TaskStatus status) {
        List<Task> tasks = (status != null)
            ? taskRepository.findByStatus(status)
            : taskRepository.findAll();
        return tasks.stream().map(TaskResponse::from).toList();
    }
}