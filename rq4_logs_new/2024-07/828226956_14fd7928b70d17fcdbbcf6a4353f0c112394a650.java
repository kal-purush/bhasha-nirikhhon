package adhd.diary.auth.exception;

import adhd.diary.response.ErrorResponse;
import java.time.LocalDateTime;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.context.request.WebRequest;

@ControllerAdvice
public class AuthExceptionHandler {

    @ExceptionHandler(KakaoTokenRequestFailedException.class)
    public ResponseEntity<ErrorResponse> handleKakaoTokenRequestFailedException(KakaoTokenRequestFailedException exception, WebRequest request) {
        ErrorResponse errorResponse = new ErrorResponse(
                LocalDateTime.now(),
                exception.getResponseCode().name(),
                exception.getResponseCode().getMessage(),
                request.getDescription(false)
        );
        return new ResponseEntity<>(errorResponse, exception.getResponseCode().getHttpStatus());
    }

    @ExceptionHandler(KakaoMemberRequestFailedException.class)
    public ResponseEntity<ErrorResponse> handleKakaoMemberRequestFailedException(KakaoMemberRequestFailedException exception, WebRequest request) {
        ErrorResponse errorResponse = new ErrorResponse(
                LocalDateTime.now(),
                exception.getResponseCode().name(),
                exception.getResponseCode().getMessage(),
                request.getDescription(false)
        );
        return new ResponseEntity<>(errorResponse, exception.getResponseCode().getHttpStatus());
    }
}