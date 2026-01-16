package org.example.advice;

import lombok.AllArgsConstructor;
import lombok.NoArgsConstructor;
import org.example.advice.enums.ExceptionAnswer;

import java.time.LocalDateTime;


@AllArgsConstructor
@NoArgsConstructor
public class ExceptionView {
    private ExceptionAnswer exception;
    private LocalDateTime timestamp;
    private String message;

    public ExceptionView(ExceptionAnswer exception, String message) {
        this.exception = exception;
        this.timestamp = LocalDateTime.now();
        this.message = message;
    }

    public ExceptionView(ExceptionAnswer exception) {
        this.exception = exception;
        this.timestamp = LocalDateTime.now();
    }

    public void setException(ExceptionAnswer exception) {
        this.exception = exception;
    }

    public void setTimestamp(LocalDateTime timestamp){
        this.timestamp = timestamp;
    }

    public void setMessage(String message){
        this.message = message;
    }
}