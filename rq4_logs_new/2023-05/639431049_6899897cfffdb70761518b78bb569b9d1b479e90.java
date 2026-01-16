package com.self.egoboard.domain.user.api;

import com.self.egoboard.domain.user.application.UserService;
import com.self.egoboard.domain.user.dto.request.UserSignUpReqDto;
import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/v1/user")
@Slf4j
public class UserController {

  private final UserService userService;

  @PostMapping("/sign-up")
  public ResponseEntity<Void> signUp(@RequestBody @Valid UserSignUpReqDto reqDto) {
    userService.signUp(reqDto);
    return ResponseEntity.status(HttpStatus.CREATED).build();
  }
}