package com.example.chronos_node.dto;

import com.example.chronos_node.model.Task;
import com.example.chronos_node.model.TaskStatus;

import java.time.LocalDateTime;
import java.util.UUID;

/**
 * API response DTO — decouples the HTTP contract from the JPA entity.
 * Never expose the entity directly from a controller.
 */
public class TaskResponse {

    private UUID id;
    private String name;
    private TaskStatus status;
    private LocalDateTime scheduledAt;
    private LocalDateTime createdAt;
    private int retryCount;

    public static TaskResponse from(Task task) {
        TaskResponse r = new TaskResponse();
        r.id          = task.getId();
        r.name        = task.getName();
        r.status      = task.getStatus();
        r.scheduledAt = task.getScheduledAt();
        r.createdAt   = task.getCreatedAt();
        r.retryCount  = task.getRetryCount();
        return r;
    }

    public UUID getId()                 { return id; }
    public String getName()             { return name; }
    public TaskStatus getStatus()       { return status; }
    public LocalDateTime getScheduledAt() { return scheduledAt; }
    public LocalDateTime getCreatedAt() { return createdAt; }
    public int getRetryCount()          { return retryCount; }
}