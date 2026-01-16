package com.yandex.kanban.server;

import com.google.gson.Gson;
import com.yandex.kanban.model.Epic;
import com.yandex.kanban.model.Status;
import com.yandex.kanban.model.Subtask;
import com.yandex.kanban.model.Task;
import com.yandex.kanban.servers.HttpTaskServer;
import com.yandex.kanban.service.InMemoryTaskManager;
import com.yandex.kanban.service.Managers;
import com.yandex.kanban.service.TaskManager;
import org.junit.After;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class HttpTaskServerTest {

    TaskManager manager = new InMemoryTaskManager();
    HttpTaskServer taskServer = new HttpTaskServer(manager);
    Gson gson = HttpTaskServer.getGson();

    public HttpTaskServerTest() throws IOException {
    }

    @BeforeEach
    public void setUp() throws IOException {
        manager.removeAllTasks();
        manager.removeAllEpics();
        manager.removeAllSubtasks();
        taskServer.start();
    }

    @AfterEach
    public void shutDown() {
        taskServer.stop();
    }

    @Test
    public void addTaskTest() throws IOException, InterruptedException {
        Task task = new Task("Task", "Testing task 2", Status.NEW, LocalDateTime.now(), Duration.ofMinutes(60));
        String taskJson = gson.toJson(task);

        HttpClient client = HttpClient.newHttpClient();
        URI url = URI.create("http://localhost:8080/tasks");
        HttpRequest request = HttpRequest.newBuilder().uri(url).POST(HttpRequest.BodyPublishers.ofString(taskJson)).build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(201, response.statusCode());

        List<Task> tasksFromManager = manager.getAllTasks();

        assertNotNull(tasksFromManager, "Задачи не возвращаются");
        assertEquals(1, tasksFromManager.size(), "Некорректное количество задач");
        assertEquals("Task", tasksFromManager.getFirst().getName(), "Некорректное имя задачи");
    }

    @Test
    public void addEpicTest() throws IOException, InterruptedException {
        Epic epic = new Epic("Epic", "Описание эпика", Status.NEW);
        String epicJson = gson.toJson(epic);
        HttpClient client = HttpClient.newHttpClient();
        URI url = URI.create("http://localhost:8080/epics");
        HttpRequest request = HttpRequest.newBuilder().uri(url).POST(HttpRequest.BodyPublishers.ofString(epicJson)).build();

        HttpResponse<String> response = client.send(request, HttpResponse.BodyHandlers.ofString());
        assertEquals(201, response.statusCode());

        List<Epic> tasksFromManager = manager.getAllEpics();

        assertNotNull(tasksFromManager, "Задачи не возвращаются");
        assertEquals(1, tasksFromManager.size(), "Некорректное количество задач");
        assertEquals("Epic", tasksFromManager.getFirst().getName(), "Некорректное имя задачи");
    }

    @Test
    public void addSubtaskTest() throws IOException, InterruptedException {
        Epic epic = new Epic("Epic", "Описание эпика", Status.NEW);
        Subtask subtask = new Subtask("Subtask", "Описание сабтаска", 1);
        String epicJson = gson.toJson(epic);
        String subtaskJson = gson.toJson(subtask);
        HttpClient client = HttpClient.newHttpClient();
        URI urlEpic = URI.create("http://localhost:8080/epics");
        URI urlSubtask = URI.create("http://localhost:8080/subtasks");
        HttpRequest requestEpic = HttpRequest.newBuilder().uri(urlEpic).POST(HttpRequest.BodyPublishers.ofString(epicJson)).build();
        HttpRequest requestSubtask = HttpRequest.newBuilder().uri(urlSubtask).POST(HttpRequest.BodyPublishers.ofString(subtaskJson)).build();

        HttpResponse<String> responseEpic = client.send(requestEpic, HttpResponse.BodyHandlers.ofString());
        HttpResponse<String> responseSubtask = client.send(requestSubtask, HttpResponse.BodyHandlers.ofString());
        assertEquals(201, responseSubtask.statusCode());

        List<Subtask> tasksFromManager = manager.getAllSubtasks();

        assertNotNull(tasksFromManager, "Задачи не возвращаются");
        assertEquals(1, tasksFromManager.size(), "Некорректное количество задач");
        assertEquals("Subtask", tasksFromManager.getFirst().getName(), "Некорректное имя задачи");
    }

    @Test
    public void getTaskTest() throws IOException, InterruptedException {
        Task task = new Task("Таск1", "Описание");
        String taskJson = gson.toJson(task);
        HttpClient client = HttpClient.newHttpClient();
        URI urlTask = URI.create("http://localhost:8080/tasks");
        URI urlGetTaskId = URI.create("http://localhost:8080/tasks/1");
        HttpRequest addRequest = HttpRequest.newBuilder()
                .uri(urlTask)
                .POST(HttpRequest.BodyPublishers.ofString(taskJson))
                .build();
        HttpRequest getRequest = HttpRequest.newBuilder()
                .uri(urlGetTaskId)
                .GET().build();
        client.send(addRequest, HttpResponse.BodyHandlers.ofString());
        HttpResponse<String> response = client.send(getRequest, HttpResponse.BodyHandlers.ofString());
        Task returnedTask = gson.fromJson(response.body(), Task.class);
        assertEquals(200, response.statusCode());
        assertEquals(task.getName(), returnedTask.getName());
    }
}