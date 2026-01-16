package com.lolup.lolup_project;

import com.lolup.lolup_project.auth.AuthController;
import com.lolup.lolup_project.auth.AuthService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mockito;
import org.springframework.boot.test.autoconfigure.web.reactive.WebFluxTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.ApplicationContext;
import org.springframework.restdocs.RestDocumentationContextProvider;
import org.springframework.restdocs.RestDocumentationExtension;
import org.springframework.security.test.context.support.WithMockUser;
import org.springframework.test.context.junit.jupiter.SpringExtension;
import org.springframework.test.web.reactive.server.WebTestClient;

import java.security.Principal;

import static org.springframework.restdocs.webtestclient.WebTestClientRestDocumentation.documentationConfiguration;

@ExtendWith({SpringExtension.class, RestDocumentationExtension.class})
@WithMockUser
@WebFluxTest(value = AuthController.class)
class AuthControllerTest {

    private WebTestClient webTestClient;

    @MockBean
    private AuthService authService;

    @BeforeEach
    public void setUp(ApplicationContext applicationContext, RestDocumentationContextProvider restDocumentation) {
        this.webTestClient = WebTestClient.bindToApplicationContext(applicationContext)
                .configureClient()
                .baseUrl("http://lolup-api.p-e.kr/")
                .filter(documentationConfiguration(restDocumentation))
                .build();
    }

    @Test
    public void 인증_권한_체크() throws Exception {
        //given
        Principal mockPrincipal = Mockito.mock(Principal.class);

        //when

        //then

    }
}