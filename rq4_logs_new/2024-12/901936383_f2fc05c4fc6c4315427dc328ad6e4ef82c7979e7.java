package com.apexify.logic.Controller;

import com.apexify.logic.DTO.CompanyRequestDTO;
import com.apexify.logic.DTO.ContentCreatorRequestDTO;
import com.apexify.logic.DTO.LoginRequestDTO;
import com.apexify.logic.DTO.UserResponseDTO;
import com.apexify.logic.Service.UserService;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.*;

@RestController
@RequestMapping("/api/auth")
@CrossOrigin(origins = "http://localhost:5173", allowedHeaders = "*")
public class AuthController {

    private final UserService userService;

    public AuthController(UserService userService) {
        this.userService = userService;
    }

    @PostMapping("/register/content-creator")
    public ResponseEntity<UserResponseDTO> registerContentCreator(@RequestBody ContentCreatorRequestDTO requestDTO) {
        UserResponseDTO response = userService.registerContentCreator(requestDTO);
        return ResponseEntity.ok(response);
    }

    @PostMapping("/register/company")
    public ResponseEntity<UserResponseDTO> registerCompany(@RequestBody CompanyRequestDTO requestDTO) {
        UserResponseDTO response = userService.registerCompany(requestDTO);
        return ResponseEntity.ok(response);
    }

    @PostMapping("/login/content-creator")
    public ResponseEntity<UserResponseDTO> loginContentCreator(@RequestBody LoginRequestDTO requestDTO) {
        UserResponseDTO response = userService.loginContentCreator(requestDTO);
        return ResponseEntity.ok(response);
    }

    @PostMapping("/login/company")
    public ResponseEntity<UserResponseDTO> loginCompany(@RequestBody LoginRequestDTO requestDTO) {
        UserResponseDTO response = userService.loginCompany(requestDTO);
        return ResponseEntity.ok(response);
    }
}
