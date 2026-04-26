package com.example.chronos_node.repository;

import com.example.chronos_node.model.Task;
import com.example.chronos_node.model.TaskStatus;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;
import org.springframework.stereotype.Repository;

import java.time.LocalDateTime;
import java.util.List;
import java.util.UUID;

@Repository
public interface TaskRepository extends JpaRepository<Task, UUID> {

    /**
     * Returns all tasks ready for dispatch.
     * Uses bound enum parameter — type-safe and refactor-safe (no raw string 'PENDING').
     */
    @Query("SELECT t FROM Task t WHERE t.status = :status AND t.scheduledAt <= :now")
    List<Task> findReadyTasks(
        @Param("status") TaskStatus status,
        @Param("now") LocalDateTime now
    );

    List<Task> findByStatus(TaskStatus status);
}