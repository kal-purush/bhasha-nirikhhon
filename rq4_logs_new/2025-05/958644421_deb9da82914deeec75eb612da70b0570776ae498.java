package pl.edu.pk.student.kittysecurity.controller;

import jakarta.validation.Valid;
import org.springframework.http.HttpHeaders;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;
import pl.edu.pk.student.kittysecurity.dto.password.CreatePasswordRequestDto;
import pl.edu.pk.student.kittysecurity.dto.password.CreatePasswordResponseDto;
import pl.edu.pk.student.kittysecurity.entity.User;
import pl.edu.pk.student.kittysecurity.exception.custom.UserNotFoundException;
import pl.edu.pk.student.kittysecurity.services.PasswordService;

import java.util.Optional;

@RestController
@RequestMapping("/api/v1/password")
public class PasswordController {

    PasswordService passwordService;

    public PasswordController(PasswordService passwordService) {
        this.passwordService = passwordService;
    }

    @PostMapping("")
    public ResponseEntity<CreatePasswordResponseDto> addPassword(@RequestHeader(HttpHeaders.AUTHORIZATION) String jwtToken, @Valid @RequestBody CreatePasswordRequestDto request){
        return passwordService.addPasswordByJwt(jwtToken, request);
    }

}