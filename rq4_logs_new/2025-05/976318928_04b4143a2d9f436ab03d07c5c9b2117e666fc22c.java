package dev.codingstoic.receiptwise.service;

import dev.codingstoic.receiptwise.dto.AuthResponse;
import dev.codingstoic.receiptwise.dto.LoginRequest;
import dev.codingstoic.receiptwise.dto.RegisterRequest;
import dev.codingstoic.receiptwise.model.User;
import dev.codingstoic.receiptwise.repository.UserRepository;
import dev.codingstoic.receiptwise.security.JwtService;
import lombok.NonNull;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.userdetails.UsernameNotFoundException;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

@Slf4j
@Service
@RequiredArgsConstructor
public class AuthService {

    private final JwtService jwtService;
    private final UserRepository userRepository;
    private final PasswordEncoder passwordEncoder;
    private final AuthenticationManager authenticationManager;

    public User register(@NonNull RegisterRequest request) {
        if (userRepository.findByUsername(request.username()).isPresent()) {
            throw new RuntimeException("Username already exists");
        }
        if (userRepository.findByEmail(request.email()).isPresent()) {
            throw new RuntimeException("Email already exists");
        }

        User user = new User();
        user.setUsername(request.username());
        user.setEmail(request.email());
        user.setPassword(passwordEncoder.encode(request.password()));

        return userRepository.save(user);
    }

    public AuthResponse login(@NonNull LoginRequest request) {
        authenticationManager.authenticate(new UsernamePasswordAuthenticationToken(request.usernameOrEmail(), request.password()));

        User user = userRepository.findByUsername(request.usernameOrEmail())
                .or(() -> userRepository.findByEmail(request.usernameOrEmail()))
                .orElseThrow(() -> new UsernameNotFoundException("User not found with username or email: %s".formatted(request.usernameOrEmail())));

        String token = jwtService.generateToken(user.getUsername());

        return new AuthResponse(
                user.getId().toString(),
                user.getUsername(),
                user.getEmail(),
                token
        );
    }
}