package com.ostapdev.todo.context;

import com.ostapdev.todo.controller.TaskController;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import static org.junit.jupiter.api.Assertions.assertNotNull;

@SpringBootTest
public class TaskControllerBeanTest {
    @Autowired
    private TaskController taskController;

    @Test
    public void taskControllerIsNotNull(){
        assertNotNull(taskController);
    }
}