package pintoss.giftmall.domains.user.controller;

import jakarta.servlet.http.HttpServletResponse;
import jakarta.servlet.http.HttpSession;
import lombok.RequiredArgsConstructor;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.web.bind.annotation.*;
import pintoss.giftmall.common.oauth.TokenProvider;
import pintoss.giftmall.common.responseobj.ApiResponse;
import pintoss.giftmall.domains.user.domain.User;
import pintoss.giftmall.domains.user.dto.NaverCallbackRequest;
import pintoss.giftmall.domains.user.dto.NaverUserResponse;
import pintoss.giftmall.domains.user.infra.UserRepository;
import pintoss.giftmall.domains.user.service.NaverOAuthService;

import java.io.IOException;
import java.net.URLEncoder;
import java.nio.charset.StandardCharsets;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/oauth/naver")
public class NaverOAuthController {

    private final NaverOAuthService naverOAuthService;
    private final UserRepository userRepository;
    private final TokenProvider tokenProvider;

    @Value("${NAVER_CLIENT}")
    private String clientId;

    @Value("${NAVER_CALLBACK_URL}")
    private String redirectUri;

    @Value("${NAVER_SECRET}")
    private String clientSecret;

    @GetMapping("/connect")
    public void redirectToNaverLogin(HttpSession session, HttpServletResponse response) throws IOException {
        String state = UUID.randomUUID().toString();
        session.setAttribute("oauthState", state);

        String naverLoginUrl = String.format(
                "https://nid.naver.com/oauth2.0/authorize?response_type=code&client_id=%s&redirect_uri=%s&state=%s",
                clientId, URLEncoder.encode(redirectUri, StandardCharsets.UTF_8), URLEncoder.encode(state, StandardCharsets.UTF_8)
        );

        response.sendRedirect(naverLoginUrl);
    }

    @PostMapping("/callback")
    public ApiResponse<String> connectNaver(@RequestBody NaverCallbackRequest request) {
        String code = request.getCode();
        String state = request.getState();

        String accessToken = naverOAuthService.getAccessToken(code, state);

        NaverUserResponse naverUser = naverOAuthService.getUserInfo(accessToken);

        Optional<User> userOpt = userRepository.findByEmail(naverUser.getEmail());

        if (userOpt.isPresent()) {
            User user = userOpt.get();

            user.connectNaver();
            userRepository.save(user);

            Authentication authentication = new UsernamePasswordAuthenticationToken(user.getEmail(), null, List.of(new SimpleGrantedAuthority(user.getRole().name())));
            String jwtToken = tokenProvider.generateAccessToken(authentication);

            return ApiResponse.ok(jwtToken);
        } else {
            return ApiResponse.of(HttpStatus.NOT_FOUND, "유저를 찾을 수 없습니다. 회원가입 후에 연동해주세요.");
        }
    }

}