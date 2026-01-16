package io.d4rkr0n1n.sample.helper;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;

public class ResponseHelper {

  private ResponseHelper() {
  }

  public static <T> ResponseEntity<T> ok(T body) {
    return ResponseEntity.status(HttpStatus.OK).body(body);
  }

  public static <T> ResponseEntity<T> ok() {
    return ResponseEntity.status(HttpStatus.OK).build();
  }

  public static <T> ResponseEntity<T> created(T body) {
    return ResponseEntity.status(HttpStatus.CREATED).body(body);
  }

  public static <T> ResponseEntity<T> notFound() {
    return ResponseEntity.status(HttpStatus.NOT_FOUND).build();
  }  

}