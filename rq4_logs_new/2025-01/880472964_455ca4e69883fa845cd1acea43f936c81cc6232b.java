package com.yandex.tracker.service;

import org.junit.jupiter.api.BeforeEach;

public abstract class TaskManagerTest<T extends TaskManager> {
    protected T taskManager;

    @BeforeEach
    public void setUp() {
        taskManager = createTaskManager();
    }

    protected abstract T createTaskManager();

}