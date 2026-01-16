package org.example.projectapp.auth.controller;

import org.example.projectapp.auth.controller.dto.AuthenticationRequestDTO;
import org.example.projectapp.model.User;
import org.example.projectapp.repository.UserRepository;
import org.example.projectapp.auth.jwt.JwtTokenProvider;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.web.authentication.logout.SecurityContextLogoutHandler;
import org.springframework.web.bind.annotation.*;

import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping("/api/v1/auth")
public class AuthenticationRestControllerV1 {

    private final AuthenticationManager authenticationManager;
    private final UserRepository userRepository;
    private final JwtTokenProvider jwtTokenProvider;

    public AuthenticationRestControllerV1(AuthenticationManager authenticationManager, UserRepository userRepository, JwtTokenProvider jwtTokenProvider) {
        this.authenticationManager = authenticationManager;
        this.userRepository = userRepository;
        this.jwtTokenProvider = jwtTokenProvider;
    }

    @PostMapping(value = "/login")
    public ResponseEntity<?> authenticate(@RequestBody AuthenticationRequestDTO request) {
        try {
            authenticationManager.authenticate(new UsernamePasswordAuthenticationToken(request.getEmail(), request.getPassword()));
            User user = userRepository.findByEmail(request.getEmail())
                    .orElseThrow(() -> new UsernameNotFoundException("User doesn't exists"));
            String accessToken = jwtTokenProvider.createToken(request.getEmail(), user.getRole().name(), false);
            String refreshToken = jwtTokenProvider.createToken(request.getEmail(), user.getRole().name(), true);
            Map<Object, Object> response = new HashMap<>();
            response.put("email", request.getEmail());
            response.put("access_token", accessToken);
            response.put("refresh_token", refreshToken);
            return ResponseEntity.ok(response);
        } catch (AuthenticationException e) {
            return new ResponseEntity<>("Invalid email/password combination", HttpStatus.FORBIDDEN);
        }
    }

    @PostMapping("/logout")
    public void logout(HttpServletRequest request, HttpServletResponse response) {
        SecurityContextLogoutHandler securityContextLogoutHandler = new SecurityContextLogoutHandler();
        securityContextLogoutHandler.logout(request, response, null);
    }

    @GetMapping("/refreshToken")
    public ResponseEntity<?> refreshToken(HttpServletRequest request) {
        String refreshToken = jwtTokenProvider.resolveToken(request);
        String userEmail = jwtTokenProvider.getUserEmail(refreshToken);
        User user = userRepository.findByEmail(userEmail)
                .orElseThrow(() -> new UsernameNotFoundException("User doesn't exists"));
        String accessToken = jwtTokenProvider.createToken(user.getEmail(), user.getRole().name(), false);
        Map<Object, Object> response = new HashMap<>();
        response.put("email", user.getEmail());
        response.put("access_token", accessToken);
        response.put("refresh_token", refreshToken);
        return ResponseEntity.ok().body(response);
    }
}