package com.example.chronos_node;

import com.example.chronos_node.model.Task;
import com.example.chronos_node.model.TaskStatus;
import com.example.chronos_node.repository.TaskRepository;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.time.LocalDateTime;

@RestController
public class TestController {

    private final TaskRepository repo;

    public TestController(TaskRepository repo) {
        this.repo = repo;
    }

    @PostMapping("/add-task")
    public String addTask(@RequestParam String name) {
        Task t = new Task();
        // Remove t.setId(...) -> Let Hibernate generate the UUID
        t.setName(name);
        t.setStatus(TaskStatus.PENDING);
        t.setScheduledAt(LocalDateTime.now());
        // Remove t.setVersion(...) -> Let Hibernate handle the versioning
        
        repo.save(t);
        return "Task Added!";
    }
}