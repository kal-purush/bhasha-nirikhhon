package it.gov.pagopa.pu.debtpositions.exception;

import it.gov.pagopa.pu.debtpositions.dto.generated.DebtPositionErrorDTO;
import it.gov.pagopa.pu.debtpositions.exception.custom.InvalidStatusException;
import jakarta.servlet.http.HttpServletRequest;
import lombok.extern.slf4j.Slf4j;
import org.springframework.core.Ordered;
import org.springframework.core.annotation.Order;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ExceptionHandler;
import org.springframework.web.bind.annotation.RestControllerAdvice;


@RestControllerAdvice
@Slf4j
@Order(Ordered.HIGHEST_PRECEDENCE)
public class DebtPositionExceptionHandler {

  @ExceptionHandler({InvalidStatusException.class})
  public ResponseEntity<DebtPositionErrorDTO> handleInternalError(RuntimeException ex, HttpServletRequest request){
    return handleWorkflowErrorException(ex, request, HttpStatus.BAD_REQUEST, DebtPositionErrorDTO.CodeEnum.IVALID_REQUEST);
  }

  static ResponseEntity<DebtPositionErrorDTO> handleWorkflowErrorException(RuntimeException ex, HttpServletRequest request, HttpStatus httpStatus, DebtPositionErrorDTO.CodeEnum errorEnum) {
    String message = logException(ex, request, httpStatus);

    return ResponseEntity
      .status(httpStatus)
      .body(DebtPositionErrorDTO.builder().code(errorEnum).message(message).build());
  }

  private static String logException(RuntimeException ex, HttpServletRequest request, HttpStatus httpStatus) {
    String message = ex.getMessage();
    log.info("A {} occurred handling request {}: HttpStatus {} - {}",
      ex.getClass(),
      getRequestDetails(request),
      httpStatus.value(),
      message);
    return message;
  }

  static String getRequestDetails(HttpServletRequest request) {
    return "%s %s".formatted(request.getMethod(), request.getRequestURI());
  }
}