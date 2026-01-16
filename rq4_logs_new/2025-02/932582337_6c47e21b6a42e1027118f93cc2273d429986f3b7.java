package com.j30n.stoblyx.adapter.in.web;

import com.j30n.stoblyx.common.security.JwtProvider;
import com.j30n.stoblyx.domain.user.User;
import com.j30n.stoblyx.port.in.UserUseCase;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/users")
@RequiredArgsConstructor
public class UserController {
    private final UserUseCase userUseCase;
    private final JwtProvider jwtProvider;

    @PostMapping("/register")
    public ResponseEntity<User> registerUser(@RequestBody RegisterUserRequest request) {
        User user = userUseCase.registerUser(
            request.email(),
            request.password(),
            request.name()
        );
        return ResponseEntity.ok(user);
    }

    @PostMapping("/login")
    public ResponseEntity<LoginResponse> login(@RequestBody LoginRequest request) {
        User user = userUseCase.findUserByEmail(request.email());
        userUseCase.validateUser(request.email(), request.password());
        
        String token = jwtProvider.createToken(user.getId());
        return ResponseEntity.ok(new LoginResponse(token));
    }

    record RegisterUserRequest(String email, String password, String name) {}
    record LoginRequest(String email, String password) {}
    record LoginResponse(String token) {}
} 