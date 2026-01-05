package com.example.chronos_node.model;

public enum TaskStatus {
    PENDING,    // Ready to be picked up
    QUEUED,     // Sent to Kafka
    COMPLETED,  // Finished by Worker
    FAILED      // Something went wrong
}