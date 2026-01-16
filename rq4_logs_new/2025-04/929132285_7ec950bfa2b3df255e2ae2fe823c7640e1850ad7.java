package nl.optifit.backendservice.exception.advice;

import com.fasterxml.jackson.annotation.*;
import lombok.*;
import org.springframework.http.*;

import java.time.*;

@Builder
@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor
public class ErrorResponse {
    private String message;
    private int status;
    @JsonFormat(shape = JsonFormat.Shape.STRING, pattern = "yyyy-MM-dd HH:mm:ss")
    private LocalDateTime timestamp;

    public static ErrorResponse of(int status, Exception exception) {
        return ErrorResponse.builder()
                .message(exception.getMessage())
                .status(status)
                .timestamp(LocalDateTime.now())
                .build();
    }
}