package com.ostapdev.todo;

import com.ostapdev.todo.controller.TaskController;
import com.ostapdev.todo.dto.task.TaskDto;
import com.ostapdev.todo.service.task.TaskService;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.mockito.ArgumentMatchers.anyBoolean;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Mockito.when;

public class TaskControllerTest {
    @Mock
    private TaskService taskService;
    @InjectMocks
    private TaskController taskController;
    private AutoCloseable closeable;

    @BeforeEach
    public void open(){
        closeable = MockitoAnnotations.openMocks(this);
    }

    @AfterEach
    public void close() throws Exception {
        closeable.close();
    }

    @Test
    public void GetTasks_TasksNotEmpty_ReturnListOfTasks(){
        when(taskService.getTasks(anyBoolean(),anyString())).thenReturn(Arrays.asList(new TaskDto(1L,"1",false),new TaskDto(2L,"2",false)));
        assertFalse(taskController.getTasks(true, "").isEmpty());
    }
}