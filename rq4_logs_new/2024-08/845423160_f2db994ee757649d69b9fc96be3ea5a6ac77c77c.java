package fiveguys.innout.controller;

import fiveguys.innout.dto.joinDto;
import fiveguys.innout.dto.loginDto;
import fiveguys.innout.entity.User;
import fiveguys.innout.repository.UserRepository;
//import fiveguys.innout.security.JwtTokenUtil;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.config.annotation.authentication.builders.AuthenticationManagerBuilder;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.web.bind.annotation.*;

@RequiredArgsConstructor
@RestController
@RequestMapping("/api/innout")
public class UserController {

    private final PasswordEncoder passwordEncoder;
    private final UserRepository userRepository;
    private AuthenticationManagerBuilder authenticationManagerBuilder;


//    @Autowired
//    private JwtTokenUtil jwtTokenUtil;

    @PostMapping("/join")
    public String join(@RequestBody joinDto joinDto) {
        if (userRepository.findByEmail(joinDto.getEmail()) != null) {
            throw new RuntimeException("Email already exists");
        }
        if (!joinDto.checkPassword()) {
            throw new RuntimeException("Passwords do not match");
        }

        User user = new User();
        user.setEmail(joinDto.getEmail());
        user.setName(joinDto.getUsername());
//        user.setPassword(joinDto.getPassword());
        user.setPassword(passwordEncoder.encode(joinDto.getPassword()));
        userRepository.save(user);

        return "User created successfully";
    }

   

//
}