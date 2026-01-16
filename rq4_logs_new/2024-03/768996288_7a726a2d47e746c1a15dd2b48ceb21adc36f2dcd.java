package api.common.util.http;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import java.util.HashMap;
import java.util.Map;

public class HttpResponse {

    public static ResponseEntity<Object> successOk(String message, Object data) {
        return buildResponse(HttpStatus.OK, message, data);
    }

    public static ResponseEntity<Object> successCreated(String message, Object data) {
        return buildResponse(HttpStatus.CREATED, message, data);
    }

    public static ResponseEntity<Object> unauthorized(String message, Object data) {
        return buildResponse(HttpStatus.UNAUTHORIZED, message, data);
    }

    public static ResponseEntity<Object> notFound(String message, Object data) {
        return buildResponse(HttpStatus.NOT_FOUND, message, data);
    }

    public static ResponseEntity<Object> badRequest(String message, Object data) {
        return buildResponse(HttpStatus.BAD_REQUEST, message, data);
    }

    public static ResponseEntity<Object> internalError(String message, Object data) {
        return buildResponse(HttpStatus.INTERNAL_SERVER_ERROR, message, data);
    }

    private static ResponseEntity<Object> buildResponse(HttpStatus status, String message, Object data) {
        Map<String, Object> body = new HashMap<>();
        body.put("status", status.value());
        body.put("message", message);
        body.put("data", data);
        return new ResponseEntity<>(body, status);
    }
}
