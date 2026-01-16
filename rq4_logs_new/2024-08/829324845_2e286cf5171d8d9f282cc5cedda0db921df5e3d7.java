package pintoss.giftmall.domains.user.controller;

import jakarta.validation.Valid;
import lombok.RequiredArgsConstructor;
import org.springframework.validation.annotation.Validated;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;
import pintoss.giftmall.common.responseobj.ApiResponse;
import pintoss.giftmall.domains.user.dto.LoginRequest;
import pintoss.giftmall.domains.user.dto.LoginResponse;
import pintoss.giftmall.domains.user.service.AuthService;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/admin")
@Validated
public class AuthAdminController {

    private final AuthService authService;

    @PostMapping("/login")
    public ApiResponse<LoginResponse> login(@RequestBody @Valid LoginRequest request) {
        LoginResponse response = authService.adminLogin(request);
        return ApiResponse.ok(response);
    }

}