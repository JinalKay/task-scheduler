package com.example.chronos_node.exception;

import jakarta.persistence.EntityNotFoundException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;

import java.time.LocalDateTime;
import java.util.List;
import java.util.Map;

@RestControllerAdvice
public class GlobalExceptionHandler {

    private static final Logger log = LoggerFactory.getLogger(GlobalExceptionHandler.class);

    /**
     * Handles @Valid failures on request bodies — returns which fields failed and why.
     * Example: POST /api/v1/tasks with blank name → 400 with field errors.
     */
    @ExceptionHandler(MethodArgumentNotValidException.class)
    public ResponseEntity<Map<String, Object>> handleValidation(MethodArgumentNotValidException ex) {
        List<String> errors = ex.getBindingResult().getFieldErrors().stream()
            .map(fe -> fe.getField() + ": " + fe.getDefaultMessage())
            .toList();

        return ResponseEntity.badRequest().body(Map.of(
            "status",    400,
            "error",     "Validation failed",
            "messages",  errors,
            "timestamp", LocalDateTime.now().toString()
        ));
    }

    /**
     * Handles GET /api/v1/tasks/{id} when the UUID doesn't exist in the DB.
     */
    @ExceptionHandler(EntityNotFoundException.class)
    public ResponseEntity<Map<String, Object>> handleNotFound(EntityNotFoundException ex) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(Map.of(
            "status",    404,
            "error",     "Not found",
            "message",   ex.getMessage(),
            "timestamp", LocalDateTime.now().toString()
        ));
    }

    /**
     * Catch-all — prevents stack traces leaking to the client.
     */
    @ExceptionHandler(Exception.class)
    public ResponseEntity<Map<String, Object>> handleAll(Exception ex) {
        log.error("Unhandled exception", ex);
        return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of(
            "status",    500,
            "error",     "Internal server error",
            "timestamp", LocalDateTime.now().toString()
        ));
    }
}