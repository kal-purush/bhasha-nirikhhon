package com.yandex.kanban.Servers;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpServer;
import com.yandex.kanban.model.Task;
import com.yandex.kanban.service.Managers;
import com.yandex.kanban.service.TaskManager;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.util.List;


public class HttpTaskServer {
    public static final int PORT = 8080;
    private final Gson gson;
    private final TaskManager taskManager;
    private static final Charset DEFAULT_CHARSET = StandardCharsets.UTF_8;
    private final HttpServer server;


    public HttpTaskServer(TaskManager taskManager) throws IOException {
        server = HttpServer.create(new InetSocketAddress("localhost", PORT), 0);
        this.taskManager = taskManager;
        gson = HttpTaskServer.getGson();
        server.createContext("/task", this::handleTask);
    }

    public static Gson getGson() {
        GsonBuilder gsonBuilder = new GsonBuilder();
        return gsonBuilder.create();
    }

    private void mainHandle(HttpExchange exchange) throws IOException {
        try (exchange) {
            System.out.println("\n/tasks: " + exchange.getRequestURI());
            String path = exchange.getRequestURI().getPath().substring(6);
            switch (path) {
                case "/prioritized":
                    prioritizedHandle(exchange);
                    break;
                case "/tasks":
                    handleTask(exchange);
                    break;
            }

        }
    }

    private void handleTask(HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        String query = exchange.getRequestURI().getQuery();
        String response = "";
        String idText;
        int id;
        Task task;
        switch (method) {
            case "GET":
                if (query == null) {
                    final List<Task> tasks = taskManager.getAllTasks();
                    response = gson.toJson(tasks);
                    System.out.println("Был запрос на получение всех задач");
                    writeResponse(exchange, response, 200);
                    return;
                }
                idText = query.substring(3);
                id = Integer.parseInt(idText);
                task = taskManager.getTask(id);
                response = gson.toJson(task);
                System.out.println("Был запрос на получений задачи c id: " + id);
                writeResponse(exchange, response, 200);
                break;
            case "DELETE":
                if (query == null) {
                    taskManager.removeAllTasks();
                    System.out.println("Был запрос на удаление всех задач");
                    exchange.sendResponseHeaders(201, 0);
                    return;
                }
                idText = query.substring(3);
                id = Integer.parseInt(idText);
                taskManager.removeTaskById(id);
                System.out.println("Удалили задачу с id: " + id);
                exchange.sendResponseHeaders(201, 0);
                break;
        }
    }

    private void prioritizedHandle (HttpExchange exchange) throws IOException {
        String method = exchange.getRequestMethod();
        if (!method.equals("GET")) {
            System.out.println("Введен неверный HTTP-метод");
            exchange.sendResponseHeaders(405,0);
        }
        final String response = gson.toJson(taskManager.getPrioritizedTasks());
        writeResponse(exchange, response, 200);
    }

    private void writeResponse(HttpExchange exchange,
                               String responseString, int responseCode) throws IOException {
        try (OutputStream os = exchange.getResponseBody()) {
            exchange.getResponseHeaders().add("Content-Type", "application/json");
            exchange.sendResponseHeaders(responseCode, 0);
            os.write(responseString.getBytes(DEFAULT_CHARSET));
        }
        exchange.close();
    }

    public void start() {
        System.out.println("Запускаем сервер на порту " + PORT);
        System.out.println("Открой в браузере http://localhost:" + PORT + "/");
        server.start();
    }

    public void stop(int time) {
        System.out.println("Останавливаем работу сервера на порту" + PORT);
        server.stop(0);
    }

    protected String readText(HttpExchange h) throws IOException {
        return new String(h.getRequestBody().readAllBytes(), DEFAULT_CHARSET);
    }


    public static void main(String[] args) throws IOException {
        HttpTaskServer httpTaskServer = new HttpTaskServer(Managers.getDefault());
        httpTaskServer.start();
//        httpTaskServer.stop(10);
    }
}

