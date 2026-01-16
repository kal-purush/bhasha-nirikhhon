package com.bearAndPupperCo.sangenWrestlingApp.Security.Controller;

import com.bearAndPupperCo.sangenWrestlingApp.Security.DTO.SignupRequest;
import com.bearAndPupperCo.sangenWrestlingApp.Security.Entity.Role;
import com.bearAndPupperCo.sangenWrestlingApp.Security.Entity.User;
import com.bearAndPupperCo.sangenWrestlingApp.Security.Service.RoleService;
import com.bearAndPupperCo.sangenWrestlingApp.Security.Service.UserService;
import com.bearAndPupperCo.sangenWrestlingApp.Security.Utils.JwtUtils;
import com.bearAndPupperCo.sangenWrestlingApp.TypeAdapters.LocalDateTypeAdapter;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseCookie;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.GrantedAuthority;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.crypto.password.PasswordEncoder;
import org.springframework.web.bind.annotation.*;

import java.time.LocalDate;
import java.util.*;
import java.util.stream.Collectors;

@CrossOrigin(origins = "*", maxAge = 3600)
@RestController
@RequestMapping("/teiai-api/auth")
public class AuthController {

    private final AuthenticationManager authenticationManager;

    private final UserService userService;

    private final JwtUtils jwtUtils;

    private final Gson gson = new GsonBuilder()
            .registerTypeAdapter(LocalDate.class, new LocalDateTypeAdapter())
            .create();

    public AuthController(AuthenticationManager authenticationManager,
                          UserService userService,
                          JwtUtils jwtUtils) {
        this.authenticationManager = authenticationManager;
        this.userService = userService;
        this.jwtUtils = jwtUtils;
    }

    @PostMapping("/signup")
    public ResponseEntity<String> registerUser(@RequestBody String info) {
        SignupRequest signUpRequest = gson.fromJson(info, SignupRequest.class);
        try {
            User user = userService.registerUser(signUpRequest);
            return new ResponseEntity<>(gson.toJson(user), HttpStatus.CREATED);
        } catch (RuntimeException e) {
            return new ResponseEntity<>(e.getMessage(), HttpStatus.BAD_REQUEST);
        }
    }

    /*@PostMapping("/signin")
    public ResponseEntity<?> authenticateUser(@RequestBody String info) {

        LoginRequest loginRequest = gson.fromJson(info,LoginRequest.class);

        Authentication authentication = authenticationManager
                .authenticate(new UsernamePasswordAuthenticationToken(loginRequest.getUsername(), loginRequest.getPassword()));

        SecurityContextHolder.getContext().setAuthentication(authentication);

        UserDetailsImp userDetails = (UserDetailsImp) authentication.getPrincipal();

        ResponseCookie jwtCookie = jwtUtils.generateJwtCookie(userDetails);

        List<String> roles = userDetails.getAuthorities().stream()
                .map(GrantedAuthority::getAuthority)
                .collect(Collectors.toList());

        return ResponseEntity.ok().header(HttpHeaders.SET_COOKIE, jwtCookie.toString())
                .body(gson.toJson(new UserInfo(userDetails.getUsername(), roles)));
    }*/

    @PostMapping("/signout")
    public ResponseEntity<?> logoutUser() {
        ResponseCookie cookie = jwtUtils.getCleanJwtCookie();
        return ResponseEntity.ok().header(HttpHeaders.SET_COOKIE, cookie.toString())
                .body("You've been signed out!");
    }
}