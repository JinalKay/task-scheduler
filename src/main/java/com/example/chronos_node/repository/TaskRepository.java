package com.example.chronos_node.repository;

import com.example.chronos_node.model.Task;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Repository
public interface TaskRepository extends JpaRepository<Task, UUID> {
    @Query("SELECT t FROM Task t WHERE t.status = 'PENDING' AND t.scheduledAt <= :now")
    List<Task> findReadyTasks(LocalDateTime now);
}