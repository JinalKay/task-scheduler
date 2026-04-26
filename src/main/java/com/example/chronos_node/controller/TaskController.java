package com.example.chronos_node.controller;

import com.example.chronos_node.dto.CreateTaskRequest;
import com.example.chronos_node.dto.TaskResponse;
import com.example.chronos_node.model.TaskStatus;
import com.example.chronos_node.service.TaskService;
import jakarta.validation.Valid;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

import java.util.List;
import java.util.UUID;

@RestController
@RequestMapping("/api/v1/tasks")
public class TaskController {

    private final TaskService taskService;

    public TaskController(TaskService taskService) {
        this.taskService = taskService;
    }

    /**
     * POST /api/v1/tasks
     * Create and schedule a new task.
     * Returns 201 Created with the full task object (including assigned UUID).
     */
    @PostMapping
    public ResponseEntity<TaskResponse> createTask(@Valid @RequestBody CreateTaskRequest request) {
        TaskResponse response = taskService.createTask(request);
        return ResponseEntity.status(HttpStatus.CREATED).body(response);
    }

    /**
     * GET /api/v1/tasks/{id}
     * Fetch the current status of a single task by its UUID.
     * Returns 200 OK or 404 Not Found (handled by GlobalExceptionHandler).
     */
    @GetMapping("/{id}")
    public ResponseEntity<TaskResponse> getTask(@PathVariable UUID id) {
        return ResponseEntity.ok(taskService.getTask(id));
    }

    /**
     * GET /api/v1/tasks?status=PENDING
     * List all tasks filtered by status. Useful for monitoring / dashboards.
     */
    @GetMapping
    public ResponseEntity<List<TaskResponse>> listTasks(
            @RequestParam(required = false) TaskStatus status) {
        List<TaskResponse> tasks = (status != null)
                ? taskService.getTasksByStatus(status)
                : taskService.getTasksByStatus(null);
        return ResponseEntity.ok(tasks);
    }
}