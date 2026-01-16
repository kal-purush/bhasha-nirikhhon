package kr.ac.kumoh.illdang100.tovalley.config.chat;

import static kr.ac.kumoh.illdang100.tovalley.util.CookieUtil.findCookieValue;

import java.util.Map;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpServletResponse;
import javax.servlet.http.HttpSession;
import kr.ac.kumoh.illdang100.tovalley.security.auth.PrincipalDetails;
import kr.ac.kumoh.illdang100.tovalley.security.jwt.JwtProcess;
import kr.ac.kumoh.illdang100.tovalley.security.jwt.JwtVO;
import kr.ac.kumoh.illdang100.tovalley.util.ChatUtil;
import kr.ac.kumoh.illdang100.tovalley.util.CustomResponseUtil;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.http.server.ServerHttpRequest;
import org.springframework.http.server.ServerHttpResponse;
import org.springframework.http.server.ServletServerHttpRequest;
import org.springframework.http.server.ServletServerHttpResponse;
import org.springframework.stereotype.Component;
import org.springframework.web.socket.WebSocketHandler;
import org.springframework.web.socket.server.HandshakeInterceptor;

@Component
@RequiredArgsConstructor
@Slf4j
// Spring Security와 Spring WebSocket을 함께 쓴다면 HandshakeInterceptor를 구현해 유저의 인증을 처리할 수 있다.
public class HttpHandshakeInterceptor implements HandshakeInterceptor {

    private final JwtProcess jwtProcess;

    /*
    일반적으로 웹소켓에서의 사용자 인증은 핸드셰이크 과정에서 한 번만 수행하고,
    그 이후에는 웹소켓 연결이 유지되는 동안 해당 연결을 신뢰하는 방식을 사용한다.
     */
    @Override
    public boolean beforeHandshake(ServerHttpRequest request, ServerHttpResponse response, WebSocketHandler wsHandler,
                                   Map<String, Object> attributes) throws Exception {

        // 웹소켓 세션의 생명주기는 웹소켓 연결이 시작되는 순간부터 연결이 종료되는 순간까지이다.

        HttpServletRequest req = ((ServletServerHttpRequest) request).getServletRequest();
        HttpServletResponse resp = ((ServletServerHttpResponse) response).getServletResponse();


        // "ACCESSTOKEN" 쿠키 값 추출
        String accessToken = findCookieValue(req, JwtVO.ACCESS_TOKEN).replace(JwtVO.TOKEN_PREFIX, "");
        try {
            PrincipalDetails principalDetails = jwtProcess.verify(accessToken);
            Long memberId = principalDetails.getMember().getId();

            // Spring Session을 이용하여 세션에 사용자 정보 저장
            HttpSession session = req.getSession();
            session.setAttribute(ChatUtil.MEMBER_ID, principalDetails.getMember().getId());

            return true;
        } catch (Exception e) {
            CustomResponseUtil.handleTokenVerificationFailure(resp);
        }
        return false;
//        log.debug("HttpHandshakeInterceptor.beforeHandshake 동작!!");
//        return true;
    }

    @Override
    public void afterHandshake(ServerHttpRequest request, ServerHttpResponse response, WebSocketHandler wsHandler,
                               Exception exception) {
    }
}