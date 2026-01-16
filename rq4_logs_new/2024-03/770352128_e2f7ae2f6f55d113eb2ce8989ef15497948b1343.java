package moofarm.ssuckssuck.domain.oauth.presentation;

import jakarta.servlet.http.HttpServletResponse;
import lombok.RequiredArgsConstructor;
import moofarm.ssuckssuck.domain.oauth.domain.OauthServerType;
import moofarm.ssuckssuck.domain.oauth.presentation.dto.request.OauthLoginRequest;
import moofarm.ssuckssuck.domain.oauth.presentation.dto.response.OauthLoginResponse;
import moofarm.ssuckssuck.domain.oauth.service.OauthService;
import org.hibernate.annotations.Parameter;
import org.springframework.web.bind.annotation.*;

@RequiredArgsConstructor
@RestController
@RequestMapping("/api")
public class OauthController {

    private final OauthService oauthService;

    @PostMapping("/login")
    public OauthLoginResponse login(
            @RequestBody OauthLoginRequest oauthLoginRequest,
            HttpServletResponse response
    ) {
        return oauthService.login(oauthLoginRequest, response);
    }
}