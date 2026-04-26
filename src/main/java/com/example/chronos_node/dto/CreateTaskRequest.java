package com.example.chronos_node.dto;

import jakarta.validation.constraints.Future;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.Size;

import java.time.LocalDateTime;

public class CreateTaskRequest {

    @NotBlank(message = "Task name must not be blank")
    @Size(max = 255, message = "Task name must not exceed 255 characters")
    private String name;

    /**
     * Optional scheduled time. If omitted, defaults to now (immediate dispatch).
     * Must be in the future if provided.
     */
    @Future(message = "scheduledAt must be a future date/time")
    private LocalDateTime scheduledAt;

    public String getName()                             { return name; }
    public void setName(String name)                    { this.name = name; }

    public LocalDateTime getScheduledAt()               { return scheduledAt; }
    public void setScheduledAt(LocalDateTime s)         { this.scheduledAt = s; }
}