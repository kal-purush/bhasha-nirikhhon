package com.LuuQu.medicalclinic.handler;

import com.LuuQu.medicalclinic.exception.*;
import com.LuuQu.medicalclinic.model.dto.ErrorMessageDto;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.ControllerAdvice;
import org.springframework.web.bind.annotation.ExceptionHandler;

@ControllerAdvice
public class GlobalExceptionHandler {
    @ExceptionHandler(MedicalClinicException.class)
    public ResponseEntity<ErrorMessageDto> medicalClinicExceptionErrorResponse(MedicalClinicException exception) {
        return ResponseEntity.badRequest().body(new ErrorMessageDto(exception.getMessage()));
    }
    @ExceptionHandler(AppointmentException.class)
    public ResponseEntity<ErrorMessageDto> appointmentExceptionErrorResponse(AppointmentException patientException) {
        return ResponseEntity.badRequest().body(new ErrorMessageDto(patientException.getMessage()));
    }
    @ExceptionHandler(PatientException.class)
    public ResponseEntity<ErrorMessageDto> patientExceptionErrorResponse(PatientException patientException) {
        return ResponseEntity.badRequest().body(new ErrorMessageDto(patientException.getMessage()));
    }
    @ExceptionHandler(NotFoundException.class)
    public ResponseEntity<ErrorMessageDto> notFoundExceptionErrorResponse(NotFoundException exception) {
        return ResponseEntity.status(HttpStatus.NOT_FOUND).body(new ErrorMessageDto(exception.getMessage()));
    }
}