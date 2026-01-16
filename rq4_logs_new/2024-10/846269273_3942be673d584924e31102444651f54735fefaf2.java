package pdes.unq.com.APC.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import pdes.unq.com.APC.interfaces.auth.LoginRequest;
import pdes.unq.com.APC.interfaces.auth.LoginResponse;
import pdes.unq.com.APC.services.AuthService;

@RestController
@RequestMapping("/api/auth")
public class AuthController {

  @Autowired
  private AuthService authService;

  @PostMapping("/login")
  public ResponseEntity<?> login(@RequestBody LoginRequest loginRequest) {
    try {
      String token = authService.login(loginRequest.getEmail(), loginRequest.getPassword());
      return ResponseEntity.ok(new LoginResponse(token));
    } catch (RuntimeException e) {
      return ResponseEntity.status(HttpStatus.UNAUTHORIZED).body("Credenciales inválidas");
    }
  }
}