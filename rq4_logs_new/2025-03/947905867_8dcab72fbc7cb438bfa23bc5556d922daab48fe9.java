package com.pet.pet_funeral.domain.pet_funeral.socialService;

import com.pet.pet_funeral.domain.user.dto.GoogleTokenResponse;

import com.pet.pet_funeral.domain.user.repository.UserRepository;
import com.pet.pet_funeral.domain.user.service.GoogleService;
import com.pet.pet_funeral.domain.user.service.RefreshTokenService;
import okhttp3.mockwebserver.MockResponse;
import okhttp3.mockwebserver.MockWebServer;
import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.client.RestClientTest;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.web.client.RestTemplateBuilder;
import org.springframework.http.HttpEntity;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.web.client.MockRestServiceServer;
import org.springframework.web.client.RestTemplate;

import java.io.IOException;

import static org.mockito.ArgumentMatchers.*;

@SpringBootTest
@TestPropertySource(properties = {
        "google.client_id=test-client-id",
        "google.client_secret=test-secret",
        "google.redirect_uri=http://localhost/callback",
        "google.user_url=http://localhost:8081/user",
        "google.token_url=http://localhost:8081/token"
})
public class GoogleServiceTest {

    @Autowired
    private GoogleService googleService;

    @Autowired
    private RestTemplateBuilder restTemplateBuilder;

    private static MockWebServer mockWebServer;

    private final String mockAccessToken = "mockAccessToken";
    private final String mockUserId = "mockUserId";

    // 테스트하기 전에 가짜서버 시작
    @BeforeAll
    static void setupMockServer() throws IOException {
        mockWebServer = new MockWebServer();
        mockWebServer.start(8081);
    }

    // 테스트 끝나면 서버 종료
    @AfterAll
    static void shutDownMockServer() throws IOException {
        mockWebServer.shutdown();
    }

    @Test
    @DisplayName("토큰 요청 성공~")
    void 토큰_요청_성공() throws Exception {
        //given
        // 가짜 토큰 응답 JSON 문자열
        String tokenJson = """
                {
                "token_type": "Bearer",
                "access_token" : "%s",
                "scope" : "scope",
                "id_token" : "idToken",
                "expires_in" : 1000
                }
                """.formatted(mockAccessToken);
        // %s 는 문자열 값을 삽입할 자리 표시자
        // mockAccessToken 을 %s 자리에 삽입

        mockWebServer.enqueue(new MockResponse()
                .setBody(tokenJson)
                .addHeader("Content-Type", "application/json")
                .setResponseCode(200));
        // json 응답을 큐에 등록

        //when
        String accessToken = googleService.getToken("mock-code");
         // mockWebServer 로 위에서 등록한 가짜 token_url 로 요청
        //then
        Assertions.assertThat(accessToken).isEqualTo(mockAccessToken);
    }

    @Test
    @DisplayName("유저 불러오기 성공~")
    void 유저_요청_성공 () throws Exception {
        //given
        String userJson = """
                { 
                "id" : "%s",
                "email" : "이메일인데용",
                "verified_email" : false,
                "name" : "최영민",
                "given_name" : "영민",
                "family_name" : "최",
                "picture" : "사진",
                "locale" : "en-US"
                }
                """.formatted(mockUserId);

        mockWebServer.enqueue(new MockResponse()
                .setBody(userJson)
                .addHeader("Content-Type", "application/json")
                .setResponseCode(200));

        //when
        String userId = googleService.getUser(mockAccessToken);
        //then
        Assertions.assertThat(userId).isEqualTo(mockUserId);
    }

}
/**
 * 실제 요청을 보내지 않고 mocking 으로 가짜 응답을 리턴 받는다.
 * -> postForEntity() 호출을 mock
 */