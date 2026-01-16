package com.springsecurity.attendance.service;

import com.springsecurity.attendance.config.JwtService;
import com.springsecurity.attendance.dto.LoginDto;
import com.springsecurity.attendance.dto.RegisterDto;
import com.springsecurity.attendance.model.UserEntity;
import com.springsecurity.attendance.repository.UserEntityRepository;
import com.springsecurity.attendance.response.CustomResponse;
import java.util.Optional;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.AuthenticationException;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.stereotype.Service;

@Service
public class AuthenticationService {
  private final JwtService jwtService;
  private final PasswordEncoder passwordEncoder;
  private final UserEntityRepository userEntityRepository;
  private final AuthenticationManager authenticationManager;

  @Autowired
  public AuthenticationService(JwtService jwtService,PasswordEncoder passwordEncoder,UserEntityRepository userEntityRepository,AuthenticationManager authenticationManager){
    this.jwtService=jwtService;
    this.passwordEncoder=passwordEncoder;
    this.userEntityRepository=userEntityRepository;
    this.authenticationManager=authenticationManager;
  }

  public CustomResponse login(LoginDto loginDto){
    try{
      Authentication authentication = authenticationManager.authenticate(
        new UsernamePasswordAuthenticationToken(loginDto.email(),loginDto.password())
      );
      SecurityContextHolder.getContext().setAuthentication(authentication);

      String token = jwtService.generateToken(authentication);
      return new CustomResponse(true,"logged in successfully",token);
    }
    catch (AuthenticationException exception){
      return new CustomResponse(false,"user with given email/password can not be found");
    }
  }

  public CustomResponse register(RegisterDto registerDto){
    if(accountExists(registerDto.email())){
      return new CustomResponse(false,"account already exists");
    }

    UserEntity user = new UserEntity();
    user.setUsername(registerDto.username());
    user.setPassword(passwordEncoder.encode(registerDto.password()));
    user.setEmail(registerDto.email());
    user.setRole("ROLE_USER");

    userEntityRepository.save(user);
    return new CustomResponse(true,"user created successfully");
  }

  private boolean accountExists(String email){
    Optional<UserEntity> userOptional = userEntityRepository.findByEmail(email);
    return userOptional.isPresent();
  }
}