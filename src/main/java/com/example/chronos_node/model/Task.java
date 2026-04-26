package com.example.chronos_node.model;

import jakarta.persistence.*;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

import java.time.LocalDateTime;
import java.util.UUID;

@Entity
@Table(
    name = "tasks",
    indexes = {
        @Index(name = "idx_tasks_status_scheduled", columnList = "status, scheduled_at")
    }
)
public class Task {

    @Id
    @GeneratedValue(strategy = GenerationType.UUID)
    private UUID id;

    @NotBlank(message = "Task name must not be blank")
    @Size(max = 255, message = "Task name must not exceed 255 characters")
    @Column(nullable = false)
    private String name;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private TaskStatus status;

    @Column(name = "scheduled_at", nullable = false)
    private LocalDateTime scheduledAt;

    @Column(name = "created_at", nullable = false, updatable = false)
    private LocalDateTime createdAt;

    @Column(name = "retry_count", nullable = false)
    private int retryCount = 0;

    @Version
    private Long version;

    public Task() {}

    @PrePersist
    void prePersist() {
        this.createdAt = LocalDateTime.now();
        if (this.status == null) this.status = TaskStatus.PENDING;
    }

    public UUID getId()                                     { return id; }
    public void setId(UUID id)                              { this.id = id; }

    public String getName()                                 { return name; }
    public void setName(String name)                        { this.name = name; }

    public TaskStatus getStatus()                           { return status; }
    public void setStatus(TaskStatus status)                { this.status = status; }

    public LocalDateTime getScheduledAt()                   { return scheduledAt; }
    public void setScheduledAt(LocalDateTime scheduledAt)   { this.scheduledAt = scheduledAt; }

    public LocalDateTime getCreatedAt()                     { return createdAt; }

    public int getRetryCount()                              { return retryCount; }
    public void setRetryCount(int retryCount)               { this.retryCount = retryCount; }

    public Long getVersion()                                { return version; }
    public void setVersion(Long version)                    { this.version = version; }
}