package com.example.chronos_node;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.scheduling.annotation.EnableScheduling;

@SpringBootApplication
@EnableScheduling 
public class ChronosNodeApplication {

    public static void main(String[] args) {
        SpringApplication.run(ChronosNodeApplication.class, args);
    }

}