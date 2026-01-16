package com;

import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

@Component
public class TaskService {
    public static Map<Long,Task> tasks = new HashMap<>();
    private static Long TASK_ID = 0L;
    public Task saveTask(Task task) {
        Task.setId(TASK_ID);
        tasks.put(TASK_ID, task);
        TASK_ID += 1;
        return task;
    }

    public static List<Task> getTasks() {
        return new LinkedList<>(tasks.values());

}