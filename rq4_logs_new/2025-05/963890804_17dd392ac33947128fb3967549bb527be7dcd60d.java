package com.example.MusicApp.service;

import com.example.MusicApp.DTO.EditProfileRequestDTO;
import com.example.MusicApp.DTO.UserProfileResponseDTO;
import com.example.MusicApp.model.Account;
import com.example.MusicApp.model.Customer;
import com.example.MusicApp.model.User;
import com.example.MusicApp.repository.AccountRepository;
import com.example.MusicApp.repository.UserRepository;
import org.springframework.stereotype.Service;

@Service          // ← tells Spring to create a bean
public class UserService {

    private final AccountRepository accountRepo;
    private final UserRepository    userRepo;

    public UserService(AccountRepository accountRepo, UserRepository userRepo) {
        this.accountRepo = accountRepo;
        this.userRepo    = userRepo;
    }

    /* ---------- read profile ---------- */
    public UserProfileResponseDTO getProfile(String username) {
        Account acc = accountRepo.findByUsername(username)
                .orElseThrow(() -> new IllegalArgumentException("Account not found"));
        User user = acc.getUser();

        UserProfileResponseDTO dto = new UserProfileResponseDTO();
        dto.setUsername(acc.getUsername());
        dto.setEmail(acc.getEmail());
        dto.setFullName(user.getFullName());
        dto.setPhone(user.getPhone());

        // membership: check if user is a Customer
        if (user instanceof Customer customer && customer.getMembership() != null) {
            dto.setMembership(customer.getMembership().name());   // enum → String
        } else {
            dto.setMembership("BASIC");  // or some default
        }
        return dto;
    }



    /* ---------- update profile ---------- */
    public void updateProfile(String username, EditProfileRequestDTO dto) {
        Account acc = accountRepo.findByUsername(username)
                .orElseThrow(() -> new IllegalArgumentException("Account not found"));
        User user = acc.getUser();
        user.setFullName(dto.getFullName());
        user.setPhone(dto.getPhone());
        userRepo.save(user);
    }
}