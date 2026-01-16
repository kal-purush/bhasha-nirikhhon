package km.cd.backend.challenge;

import com.fasterxml.jackson.databind.ObjectMapper;
import km.cd.backend.challenge.controller.ChallengeController;
import km.cd.backend.challenge.dto.ChallengeDto;
import km.cd.backend.challenge.dto.ChallengeResponseDto;
import km.cd.backend.challenge.service.ChallengeService;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.test.web.servlet.MockMvc;
import org.springframework.test.web.servlet.setup.MockMvcBuilders;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.lenient;
import static org.mockito.Mockito.when;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.*;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;



@ExtendWith(MockitoExtension.class)
public class ChallengeControllerTest {

    @InjectMocks
    private ChallengeController challengeController;

    @Mock
    private ChallengeService challengeService;

    private MockMvc mockMvc;
    private ObjectMapper objectMapper = new ObjectMapper();

    @BeforeEach
    public void setUp() {
        mockMvc = MockMvcBuilders.standaloneSetup(challengeController).build();
    }

    @Test
    @DisplayName("챌린지 생성 폼 접근 테스트")
    public void testNewChallengeForm() throws Exception {
        // given: "/challenge/create_form" 경로에 대한 GET 요청
        // when: GET 요청 수행
        // then: HTTP 상태 코드 200 (OK) 및 뷰 이름이 "/challenge/create_form"인지 확인
        mockMvc.perform(get("/challenge/create_form"))
                .andExpect(status().isOk())
                .andExpect(view().name("/challenge/create_form2"));
    }

    @Test
    @DisplayName("챌린지 생성 테스트")
    public void testCreateChallenge() throws Exception {
        // given: ChallengeDTO 객체를 생성하여 "/challenge/create_form" 경로에 대한 POST 요청
        ChallengeDto challengeDTO = new ChallengeDto();
        ChallengeResponseDto savedChallenge = new ChallengeResponseDto();
        savedChallenge.setChallengeId(1);

        // when: POST 요청 수행 및 ChallengeRepository의 save 메서드 호출 시 가짜 Challenge 객체 반환
        // then: HTTP 상태 코드 및 리다이렉트된 URL이 "/challenge/success?challenge_id=1"인지 확인

        when(challengeService.saveChallenge(any(ChallengeDto.class))).thenReturn(savedChallenge);

        mockMvc.perform(post("/challenge/create_form")
                        .flashAttr("challengeDTO", challengeDTO))
                .andExpect(status().is3xxRedirection())
                .andExpect(redirectedUrl("/challenge/success?challenge_id=1"));
    }

    @Test
    @DisplayName("챌린지 생성 후 리다이렉트 테스트")
    public void testShowSuccessPage() throws Exception {
        // given: "/challenge/success" 경로에 대한 GET 요청과 challenge_id 매개변수 설정
        Integer challenge_id = 1;
        ChallengeResponseDto challenge = new ChallengeResponseDto();
        challenge.setChallengeId(challenge_id);

        // when: GET 요청 수행
        lenient().when(challengeService.getChallenge(challenge_id)).thenReturn(challenge);

        // then: HTTP 상태 코드 200 (OK), 뷰 이름이 "/challenge/success"이고 모델에 "challenge_id" 속성이 있는지 확인
        mockMvc.perform(get("/challenge/success")
                        .param("challenge_id", String.valueOf(challenge_id)))
                .andExpect(status().isOk())
                .andExpect(view().name("/challenge/success2"))
                .andExpect(model().attributeExists("challenge_id"));
    }
}