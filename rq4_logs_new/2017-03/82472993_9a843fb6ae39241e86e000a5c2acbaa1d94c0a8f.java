package com.kvteam.backend.dataformats;

import org.eclipse.jetty.http.HttpStatus;

/**
 * Created by maxim on 28.02.17.
 */
public class ResponseStatusData {
    public static ResponseStatusData SUCCESS = new ResponseStatusData(HttpStatus.OK_200, "success");
    public static ResponseStatusData INVALID_REQUEST = new ResponseStatusData(HttpStatus.BAD_REQUEST_400, "invalid request");
    public static ResponseStatusData ACCESS_DENIED = new ResponseStatusData(HttpStatus.FORBIDDEN_403, "Access denied");
    public static ResponseStatusData NOT_FOUND = new ResponseStatusData(HttpStatus.NOT_FOUND_404, "object not found");
    public static ResponseStatusData CONFLICT = new ResponseStatusData(HttpStatus.CONFLICT_409, "already exists");

    private int code = HttpStatus.OK_200;
    private String message = "Internal error";

    public ResponseStatusData(int c, String mess){
        code = c;
        message = mess;
    }

    public int getCode(){
        return code;
    }
    public String getMessage(){
        return message;
    }
}