package km.cd.backend.auth;

import km.cd.backend.jwt.JwtTokenResponse;
import km.cd.backend.user.dto.UserLogin;
import km.cd.backend.user.dto.UserRegister;
import lombok.RequiredArgsConstructor;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequestMapping("/auth")
@RequiredArgsConstructor
public class AuthController {

    private final AuthService authService;

    @PostMapping("/register")
    public ResponseEntity<Void> register(@RequestBody UserRegister userRegister) {
        authService.register(userRegister);
        return ResponseEntity.ok(null);
    }

    @PostMapping("/login")
    public ResponseEntity<JwtTokenResponse> login(@RequestBody UserLogin userLogin) {
        JwtTokenResponse tokenResponse = authService.login(userLogin);
        return ResponseEntity.ok(tokenResponse);
    }
}