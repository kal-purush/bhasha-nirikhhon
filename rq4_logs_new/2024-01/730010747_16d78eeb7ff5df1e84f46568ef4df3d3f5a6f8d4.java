package com.dockbang.controller;

import org.springframework.http.HttpStatus;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

@ControllerAdvice
public class GlobalExceptionHandler {

    @ExceptionHandler(Exception.class)
    public String handleException(Exception e, Model model) {
        model.addAttribute("error", "Internal Server Error: " + HttpStatus.BAD_REQUEST);
        model.addAttribute("message", "An unexpected error occurred<br>페이지에 접근할 수 없습니다.");
        System.out.println("에러: " + e.getMessage());
        return "error/error"; // 에러 페이지의 JSP 경로
    }

}