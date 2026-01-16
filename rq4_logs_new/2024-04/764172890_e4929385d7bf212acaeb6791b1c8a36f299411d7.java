package com.aeon.hadog.controller;

import com.aeon.hadog.base.dto.response.ResponseDTO;
import com.aeon.hadog.base.dto.user.JoinRequestDTO;
import com.aeon.hadog.base.dto.user.LoginRequestDTO;
import com.aeon.hadog.service.UserService;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@Controller
@RequiredArgsConstructor
@RequestMapping("/user")
public class UserController {

    private final UserService userService;

    @PostMapping("/join")
    public ResponseEntity<ResponseDTO> register(@RequestBody JoinRequestDTO joinRequestDTO) {
        Long userId = userService.signup(joinRequestDTO);

        return ResponseEntity
                .ok()
                .body(new ResponseDTO<>(200, true, null, userId));
    }

    @PostMapping("/login")
    public ResponseEntity<ResponseDTO> login(@RequestBody LoginRequestDTO loginRequestDTO){
        String token = userService.signin(loginRequestDTO);
        return ResponseEntity
                .ok()
                .body(new ResponseDTO<>(200, true, null, token));
    }
}