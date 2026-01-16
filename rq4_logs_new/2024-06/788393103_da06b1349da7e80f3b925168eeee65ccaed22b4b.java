package dk.dtu.compute.se.pisd.roborally.client;

import java.net.URI;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;

public class Client {

    private static final HttpClient HTTP_CLIENT = HttpClient.newBuilder().version(HttpClient.Version.HTTP_2).build();
    private String server = "http://localhost:8080";
    private String gameID = "";


    public void uploadGame(String gameJson){
        HttpRequest request = HttpRequest.newBuilder()
                .PUT(HttpRequest.BodyPublishers.ofString(gameJson))
                .uri(URI.create(server + "/lobby/" + gameID))
                .setHeader("Content-Type", "application/json")
                .build();
    }
}