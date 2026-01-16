package bloodbank.bloodbankservice.core.utils;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

import java.time.Instant;

public class APIResponseHandler {
    private APIResponseHandler() {
        // Private constructor to prevent class instantiation.
    }

    public static <T> ResponseEntity<APIResponse<T>> success(final T payload) {
        return success("The Operation was completed successfully", HttpStatus.OK, payload);
    }

    public static <T> ResponseEntity<APIResponse<T>> success(
            final String message,
            final HttpStatus status,
            final T payload
    ) {
        return ResponseEntity
                .status(status == null ? HttpStatus.OK : status)
                .body(APIResponse.<T>builder()
                        .message(message)
                        .status(status == null ? HttpStatus.OK : status)
                        .payload(payload)
                        .timestamp(Instant.now())
                        .build());
    }

    public static <T> ResponseEntity<APIResponse<T>> error(
            final HttpStatus status,
            final String errorMessage,
            final String errorTrace
    ) {
        return ResponseEntity
                .status(status == null ? HttpStatus.INTERNAL_SERVER_ERROR : status)
                .body(APIResponse.<T>builder()
                        .message(errorMessage)
                        .status(status == null ? HttpStatus.INTERNAL_SERVER_ERROR : status)
                        .errorTrace(errorTrace)
                        .timestamp(Instant.now())
                        .build());

    }

    public static <T> ResponseEntity<APIResponse<T>> created(
            final String message,
            final HttpStatus status,
            final T payload,
            final String errorTrace
    ) {
        if (status.is4xxClientError() || status.is5xxServerError()) {
            return error(status, message, errorTrace);
        }

        return success(message, status, payload);
    }
}