package com.unifucamp.gamearena.auth.controller;

import java.util.Optional;

import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.server.ResponseStatusException;

import com.unifucamp.gamearena.auth.dto.LoginRequestDTO;
import com.unifucamp.gamearena.auth.dto.RegisterRequestDTO;
import com.unifucamp.gamearena.auth.dto.ResponseDTO;
import com.unifucamp.gamearena.auth.service.TokenService;
import com.unifucamp.gamearena.domain.User;
import com.unifucamp.gamearena.repositories.UserRepository;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;

@RestController
@RequestMapping("/auth")
@RequiredArgsConstructor
public class AuthController {

  private final UserRepository userRepository;
  private final PasswordEncoder passwordEncoder;
  private final TokenService tokenService;

  @PostMapping("/login")
  public ResponseEntity<ResponseDTO> login(@Valid @RequestBody LoginRequestDTO body) {
    User user = userRepository.findByEmail(body.email())
        .orElseThrow(() -> new ResponseStatusException(HttpStatus.NOT_FOUND, "User not found"));

    if (passwordEncoder.matches(body.password(), user.getPassword())) {
      String token = tokenService.generateToken(user);
      return ResponseEntity.ok(new ResponseDTO(token));
    }
    throw new ResponseStatusException(HttpStatus.UNAUTHORIZED, "Invalid credentials");
  }

  @PostMapping("/register")
  public ResponseEntity<ResponseDTO> register(@Valid @RequestBody RegisterRequestDTO body) {
    Optional<User> user = userRepository.findByEmail(body.email());

    if (user.isEmpty()) {
      User newUser = new User();
      newUser.setPassword(passwordEncoder.encode(body.password()));
      newUser.setEmail(body.email());
      newUser.setName(body.name());

      userRepository.save(newUser);

      String token = tokenService.generateToken(newUser);
      return ResponseEntity.ok(new ResponseDTO(token));
    }
    return ResponseEntity.badRequest().build();
  }
}