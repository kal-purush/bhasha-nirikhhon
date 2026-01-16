package com.pesados.purplepoint.api.exception;

import org.json.JSONObject;
import org.springframework.http.HttpStatus;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.ResponseStatus;

@ControllerAdvice
class UnexpectedSQLErrorAdvice {
  @ResponseBody
  @ExceptionHandler(UnexpectedSQLError.class)
  @ResponseStatus(HttpStatus.BAD_REQUEST)
  String userNotFoundHandler(UnexpectedSQLError ex) {
    JSONObject ret = new JSONObject();
    ret.put("message", ex.getMessage());
    ret.put("status", HttpStatus.BAD_REQUEST);
    ret.put("error", "SQL opeartion");
    return ret.toString();
  }
}