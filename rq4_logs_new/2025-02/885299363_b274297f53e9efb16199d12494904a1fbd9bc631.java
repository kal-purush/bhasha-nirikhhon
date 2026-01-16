package com.yandex.kanban.model;

import com.yandex.kanban.service.Managers;
import com.yandex.kanban.service.TaskManager;
import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class TaskTest {
    TaskManager inMemoryTaskManager = Managers.getDefault();
    @Test
    void checkEpicStatusAllSubtasksNew(){
        Epic epic = new Epic("Epic1", "Описание1");
        inMemoryTaskManager.createEpic(epic);
        Subtask subtask1 = new Subtask("Сабтаск1", "Описание1", Status.NEW, epic.getId());
        Subtask subtask2 = new Subtask("Сабтаск2", "Описание2", Status.NEW, epic.getId());
        inMemoryTaskManager.createSubtask(subtask1);
        inMemoryTaskManager.createSubtask(subtask2);
        assertEquals(Status.NEW, epic.getStatus());
    }

    @Test
    void checkEpicStatusAllSubtasksDone(){
        Epic epic = new Epic("Epic1", "Описание1");
        inMemoryTaskManager.createEpic(epic);
        Subtask subtask1 = new Subtask("Сабтаск1", "Описание1", Status.DONE, epic.getId());
        Subtask subtask2 = new Subtask("Сабтаск2", "Описание2", Status.DONE, epic.getId());
        inMemoryTaskManager.createSubtask(subtask1);
        inMemoryTaskManager.createSubtask(subtask2);
        assertEquals(Status.DONE, epic.getStatus());
    }

    @Test
    void checkEpicStatusSubtasksDoneAndNew() {
        Epic epic = new Epic("Epic1", "Описание1");
        inMemoryTaskManager.createEpic(epic);
        Subtask subtask1 = new Subtask("Сабтаск1", "Описание1", Status.NEW, epic.getId());
        Subtask subtask2 = new Subtask("Сабтаск2", "Описание2", Status.DONE, epic.getId());
        inMemoryTaskManager.createSubtask(subtask1);
        inMemoryTaskManager.createSubtask(subtask2);
        assertEquals(Status.IN_PROGRESS, epic.getStatus());
    }

    @Test
    void checkEpicStatusSubtasksInProgress() {
        Epic epic = new Epic("Epic1", "Описание1");
        inMemoryTaskManager.createEpic(epic);
        Subtask subtask1 = new Subtask("Сабтаск1", "Описание1", Status.IN_PROGRESS, epic.getId());
        Subtask subtask2 = new Subtask("Сабтаск2", "Описание2", Status.IN_PROGRESS, epic.getId());
        inMemoryTaskManager.createSubtask(subtask1);
        inMemoryTaskManager.createSubtask(subtask2);
        assertEquals(Status.IN_PROGRESS, epic.getStatus());
    }
}