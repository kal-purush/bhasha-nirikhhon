package com.repositories.impl;


import com.models.Task;
import com.repositories.ToDoInterface;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

public class TaskRepository implements ToDoInterface {
    private static Map<Long, Task> tasks = new HashMap<>();

   public TaskRepository(Map<Long, Task> tasks){
   this.tasks=tasks;
   }

    private static Long TASK_ID = 0L;
    public Task saveTask(Task task) {
        Task.setId(TASK_ID);
        tasks.put(TASK_ID, task);
        TASK_ID += 1;
        return task;
    }

    public List<Task> getTasks() {
        return new LinkedList<>(tasks.values());
    }

    }