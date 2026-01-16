package org.example.projectapp.controller;


import org.example.projectapp.controller.dto.RegisterUserDto;
import org.example.projectapp.controller.dto.ValidationErrorResponse;
import org.example.projectapp.model.User;
import org.example.projectapp.service.RegistrationService;
import org.example.projectapp.service.ValidationService;
import org.example.projectapp.service.exception.UserAlreadyExistsException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.validation.FieldError;
import org.springframework.web.bind.MethodArgumentNotValidException;
import org.springframework.web.bind.annotation.*;

import javax.validation.Valid;
import java.util.*;

@RestController
@RequestMapping("/registration")
public class RegistrationController {
    Logger logger = LoggerFactory.getLogger(RegistrationController.class);

    private final RegistrationService registrationService;
    private final ValidationService validationService;

    @Autowired
    public RegistrationController(RegistrationService registrationService, ValidationService validationService) {
        this.registrationService = registrationService;
        this.validationService = validationService;
    }

    @PostMapping(produces = MediaType.APPLICATION_JSON_VALUE)
    public ResponseEntity<User> register(@RequestBody @Valid RegisterUserDto registerDto) {
        User register = registrationService.register(registerDto);
        return ResponseEntity.ok(register);
    }

    @ExceptionHandler(UserAlreadyExistsException.class)
    public ResponseEntity<RegisterUserDto> userAlreadyExists(UserAlreadyExistsException exc) {
        RegisterUserDto registerDto = exc.getRegisterDto();
        logger.info("[REGISTRATION] User already exists with email={}", registerDto.getEmail());
        return ResponseEntity.ok(registerDto);
    }

    @ExceptionHandler(MethodArgumentNotValidException.class)
    @ResponseBody
    public ResponseEntity<ValidationErrorResponse> onMethodArgumentNotValidException(MethodArgumentNotValidException e) {
        List<FieldError> fieldErrors = e.getBindingResult().getFieldErrors();
        ValidationErrorResponse validationErrorResponse = validationService.mapErrors(fieldErrors);
        logger.info("[VALIDATION] Fields contain validation errors {}", validationErrorResponse.getErrors());
        return ResponseEntity
                .status(HttpStatus.BAD_REQUEST)
                .body(validationErrorResponse);
    }

}