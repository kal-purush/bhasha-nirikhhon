package server.handlers;

import spark.Response;

import java.util.Objects;

public class ExceptionHandler {
    public void changeStatus(Response response, Exception e){
        String message = e.getMessage();
        if(Objects.equals(message, "Error: unauthorized")){
            response.status(401);
        }
        if(Objects.equals(message, "Error: bad request")){
            response.status(400);
        }
        if(Objects.equals(message, "Error: already taken")){
            response.status(403);
        }

    }
}