package io.github.stuartwdouglas.hacbstest;

import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

import java.io.IOException;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;

public class Main {

    public static final String HELLO = "Hello World";

    public static void main(String... args) throws IOException {
        HttpServer server = HttpServer.create(new InetSocketAddress("0.0.0.0", 8080), 100);
        server.createContext("/").setHandler(new HttpHandler() {
            @Override
            public void handle(HttpExchange exchange) throws IOException {
                exchange.sendResponseHeaders(200, HELLO.length());
                try (OutputStream responseBody = exchange.getResponseBody();) {
                    responseBody.write(HELLO.getBytes(StandardCharsets.UTF_8));
                }
            }
        });
        System.out.println("Starting HTTP server on port 8080");
        server.start();
    }

}