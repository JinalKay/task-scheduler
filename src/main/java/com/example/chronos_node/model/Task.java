package com.example.chronos_node.model;

import jakarta.persistence.*;
import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(name = "tasks")
public class Task {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    private String name;

    @Enumerated(EnumType.STRING)
    private TaskStatus status;

    private LocalDateTime scheduledAt;

    @Version
    private Long version;

    // --- Manual Getters and Setters (Since Lombok failed) ---
    public Task() {}

    public Task(UUID id, String name, TaskStatus status, LocalDateTime scheduledAt, Long version) {
        this.id = id;
        this.name = name;
        this.status = status;
        this.scheduledAt = scheduledAt;
        this.version = version;
    }

    public UUID getId() { return id; }
    public void setId(UUID id) { this.id = id; }

    public String getName() { return name; }
    public void setName(String name) { this.name = name; }

    public TaskStatus getStatus() { return status; }
    public void setStatus(TaskStatus status) { this.status = status; }

    public LocalDateTime getScheduledAt() { return scheduledAt; }
    public void setScheduledAt(LocalDateTime scheduledAt) { this.scheduledAt = scheduledAt; }

    public Long getVersion() { return version; }
    public void setVersion(Long version) { this.version = version; }
}