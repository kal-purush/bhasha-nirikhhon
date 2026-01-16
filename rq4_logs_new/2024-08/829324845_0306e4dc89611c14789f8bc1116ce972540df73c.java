package pintoss.giftmall.domains.user.controller;

import jakarta.validation.Valid;
import jakarta.validation.constraints.NotBlank;
import lombok.RequiredArgsConstructor;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.*;
import pintoss.giftmall.common.responseobj.ApiResponse;
import pintoss.giftmall.domains.user.dto.LoginRequest;
import pintoss.giftmall.domains.user.dto.LoginResponse;
import pintoss.giftmall.domains.user.dto.RegisterRequest;
import pintoss.giftmall.domains.user.service.AuthService;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/user")
@Validated
public class AuthController {

    private final AuthService authService;

    @PostMapping("/register")
    public ApiResponse<Void> register(@RequestBody @Valid RegisterRequest request) {
        authService.register(request);
        return ApiResponse.ok(null);
    }

    @PostMapping("/login")
    public ApiResponse<LoginResponse> login(@RequestBody @Valid LoginRequest request) {
        LoginResponse response = authService.login(request);
        return ApiResponse.ok(response);
    }

    @GetMapping("/check-id")
    public ApiResponse<Boolean> checkEmailDuplicate(@RequestParam @NotBlank String email) {
        boolean isDuplicate = authService.checkEmailDuplicate(email);
        return ApiResponse.ok(isDuplicate);
    }

    @GetMapping("/find-id")
    public ApiResponse<String> findUserId(@RequestParam @NotBlank String name, @RequestParam @NotBlank String phone) {
        String email = authService.findUserIdByNameAndPhone(name, phone);
        return ApiResponse.ok(email);
    }

    @GetMapping("/find-password")
    public ApiResponse<Void> findPassword(@RequestParam @NotBlank String name, @RequestParam @NotBlank String email) {
        authService.findPassword(name, email);
        return ApiResponse.ok(null);
    }

}