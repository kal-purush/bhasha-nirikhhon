package leets.weeth.global.sas.presentation;

import jakarta.servlet.http.HttpServletRequest;
import jakarta.servlet.http.HttpServletResponse;
import leets.weeth.domain.user.domain.entity.SecurityUser;
import leets.weeth.domain.user.domain.entity.User;
import leets.weeth.global.sas.application.dto.OauthUserInfoResponse;
import leets.weeth.global.sas.application.usecase.AuthUsecase;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.security.authentication.UsernamePasswordAuthenticationToken;
import org.springframework.security.core.Authentication;
import org.springframework.security.core.authority.SimpleGrantedAuthority;
import org.springframework.security.core.context.SecurityContext;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.security.web.authentication.SavedRequestAwareAuthenticationSuccessHandler;
import org.springframework.security.web.context.SecurityContextRepository;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestHeader;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
@RequiredArgsConstructor
public class AuthController {

    private final SavedRequestAwareAuthenticationSuccessHandler savedRequestHandler;
    private final AuthUsecase authUsecase;
    private final SecurityContextRepository securityContextRepository;

    @GetMapping("/kakao/oauth")
    public void kakaoCallback(@RequestParam String code,
                              HttpServletRequest request,
                              HttpServletResponse response) throws Exception {

        User findUser = authUsecase.login(code);

        Authentication auth = new UsernamePasswordAuthenticationToken(SecurityUser.from(findUser), null, List.of(new SimpleGrantedAuthority(findUser.getRole().name())));

        SecurityContext context = SecurityContextHolder.createEmptyContext();
        context.setAuthentication(auth);
        SecurityContextHolder.setContext(context);
        securityContextRepository.saveContext(context, request, response);

        savedRequestHandler.onAuthenticationSuccess(request, response, auth);
    }

    @GetMapping("/user/me")
    public OauthUserInfoResponse userInfo(@RequestHeader("Authorization") String accessToken) {
        return authUsecase.userInfo(accessToken);
    }
}