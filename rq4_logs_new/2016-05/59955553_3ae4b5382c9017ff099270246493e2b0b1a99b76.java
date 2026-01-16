package net.bluetab.bigdataactivity.http_server;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.UnsupportedEncodingException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.net.URLDecoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.sun.net.httpserver.Headers;
import com.sun.net.httpserver.HttpExchange;
import com.sun.net.httpserver.HttpHandler;
import com.sun.net.httpserver.HttpServer;

public class CassandraLogHttpServer {
   private int port;
   private HttpServer server;

   public void start() {
      try {
         this.port = 80;
         server = HttpServer.create(new InetSocketAddress(port), 0);
         System.err.println("server started at " + port);
         Handlers.DbPostHandler handler = new Handlers.DbPostHandler();
         handler.connect();
         server.createContext("/", handler);
         server.setExecutor(null);
         server.start();
      } catch (IOException e) {
         System.err.println(e.getMessage());
      }
   }

   public void stop() {
      server.stop(0);
      System.err.println("server stopped");
   }
}