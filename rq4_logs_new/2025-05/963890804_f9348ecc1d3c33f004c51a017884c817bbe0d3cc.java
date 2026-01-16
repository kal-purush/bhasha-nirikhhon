package com.example.MusicApp.controller;

import com.example.MusicApp.DTO.EditProfileRequestDTO;
import com.example.MusicApp.DTO.UserProfileResponseDTO;
import com.example.MusicApp.model.Account;
import com.example.MusicApp.model.User;
import com.example.MusicApp.repository.AccountRepository;
import com.example.MusicApp.repository.UserRepository;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.annotation.AuthenticationPrincipal;
import org.springframework.security.core.userdetails.UserDetails;
import org.springframework.web.bind.annotation.*;

import java.util.Optional;

@RestController
@RequestMapping("/api/user")
public class UserController {

    @Autowired
    private AccountRepository accountRepository;

    @Autowired
    private UserRepository userRepository;

    @GetMapping("/profile")
    public ResponseEntity<UserProfileResponseDTO> getProfile(@AuthenticationPrincipal UserDetails userDetails) {
        String username = userDetails.getUsername();
        Optional<Account> accOpt = accountRepository.findByUsername(username);
        if (accOpt.isEmpty()) return ResponseEntity.notFound().build();

        Account account = accOpt.get();
        User user = account.getUser();

        UserProfileResponseDTO dto = new UserProfileResponseDTO();
        dto.setUsername(account.getUsername());
        dto.setEmail(account.getEmail());
        dto.setFullName(user.getFullName());
        dto.setPhone(user.getPhone());

        return ResponseEntity.ok(dto);
    }

    @PutMapping("/profile")
    public ResponseEntity<?> updateProfile(@RequestBody EditProfileRequestDTO dto,
                                           @AuthenticationPrincipal UserDetails userDetails) {
        String username = userDetails.getUsername();
        Optional<Account> accOpt = accountRepository.findByUsername(username);
        if (accOpt.isEmpty()) return ResponseEntity.status(404).body("Account not found");

        Account account = accOpt.get();
        User user = account.getUser();

        user.setFullName(dto.getFullName());
        user.setPhone(dto.getPhone());
        userRepository.save(user);

        return ResponseEntity.ok().build();
    }
}