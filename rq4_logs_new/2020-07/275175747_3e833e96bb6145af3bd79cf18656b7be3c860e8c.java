package jm.stockx.controller.rest.login;

import io.swagger.v3.oas.annotations.tags.Tag;
import jm.stockx.JwtUtil;
import jm.stockx.entity.User;
import jm.stockx.util.Response;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.authentication.AuthenticationManager;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import java.util.HashMap;
import java.util.Map;

@RestController
@RequestMapping(value = "/rest/api/auth")
@Tag(name = "login", description = "Login API")
@Slf4j
public class LoginRestController {

    private final AuthenticationManager authenticationManager;
    private final JwtUtil jwtUtil;

    public LoginRestController(AuthenticationManager authenticationManager, JwtUtil jwtUtil) {
        this.authenticationManager = authenticationManager;
        this.jwtUtil = jwtUtil;
    }


    @PostMapping("login")
    public Response<?> signIn(@RequestBody User user) {
        String username = user.getUsername();
        authenticationManager.authenticate(new UsernamePasswordAuthenticationToken(username, user.getPassword()));
        String token = jwtUtil.generateToken(user);
        Map<Object, Object> response = new HashMap<>();
        response.put("username", username);
        response.put("token", token);
        return Response.ok(response);
    }
}