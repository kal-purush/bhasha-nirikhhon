package com.example;

import org.springframework.ai.tool.ToolCallbackProvider;
import org.springframework.ai.tool.method.MethodToolCallbackProvider;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;

@SpringBootApplication
public class McpServerTrinoApp {

    public static void main(String[] args) {
        SpringApplication.run(McpServerTrinoApp.class, args);
    }

    @Bean
    public ToolCallbackProvider trinoSchemaTools(TrinoSchemaService trinoSchemaService) {
        return MethodToolCallbackProvider.builder().toolObjects(trinoSchemaService).build();
    }

    @Bean
    public ToolCallbackProvider trinoQueryTools(TrinoQueryService trinoQueryService) {
        return MethodToolCallbackProvider.builder().toolObjects(trinoQueryService).build();
    }


}