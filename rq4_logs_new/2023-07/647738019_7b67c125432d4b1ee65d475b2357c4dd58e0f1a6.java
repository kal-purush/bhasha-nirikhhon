package com.mhc.haveit.controller;

import com.mhc.haveit.config.TestSecurityConfig;
import com.mhc.haveit.dto.HabitRegistrationDto;
import com.mhc.haveit.service.HabitRegistrationService;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.autoconfigure.web.servlet.WebMvcTest;
import org.springframework.boot.test.mock.mockito.MockBean;
import org.springframework.context.annotation.Import;
import org.springframework.http.MediaType;
import org.springframework.security.test.context.support.TestExecutionEvent;
import org.springframework.security.test.context.support.WithUserDetails;
import org.springframework.test.web.servlet.MockMvc;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.BDDMockito.then;
import static org.mockito.BDDMockito.willDoNothing;
import static org.springframework.security.test.web.servlet.request.SecurityMockMvcRequestPostProcessors.csrf;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.*;

@DisplayName("컨트롤러 - 습관 신청")
@Import(TestSecurityConfig.class)
@WebMvcTest(HabitRegistrationController.class)
class HabitRegistrationControllerTest {
    private final MockMvc mvc;
    @MockBean private HabitRegistrationService habitRegistrationService;

    public HabitRegistrationControllerTest(@Autowired MockMvc mvc) { this.mvc = mvc; }

    @DisplayName("[POST] 습관 신청 - 인증되지않은 사용자가 신청")
    @Test
    void givenHabitId_whenRequesting_thenRedirectsToLoginPage() throws Exception {
        // Given
        String habitEncoded ="habitId=1";

        // When & Then
        mvc.perform(post("/habitRegistration/new")
                        .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                        .content(habitEncoded)
                        .with(csrf())
                )
                .andExpect(status().is3xxRedirection())
                .andExpect(redirectedUrlPattern("**/login"));
        then(habitRegistrationService).shouldHaveNoInteractions();
    }

    @DisplayName("[POST] 습관 신청 - 인증된 사용자")
    @WithUserDetails(value = "jshTest", setupBefore = TestExecutionEvent.TEST_EXECUTION)
    @Test
    void givenHabitId_whenRequesting_thenSavesHabitRegistration() throws Exception {
        // Given
        Long habitId = 1L;
        String habitEncoded ="habitId=1";

        // When & Then
        mvc.perform(post("/habitRegistration/new")
                        .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                        .content(habitEncoded)
                        .with(csrf())
                )
                .andExpect(status().is3xxRedirection())
                .andExpect(view().name("redirect:/habits/"+habitId))
                .andExpect(redirectedUrl("/habits/"+habitId));
        then(habitRegistrationService).should().saveHabitRegistration(any(HabitRegistrationDto.class));
    }

    @DisplayName("[POST] 습관 신청 취소 - 인증되지않은 사용자가 취소")
    @Test
    void givenHabitId_whenDeleting_thenRedirectsToLoginPage() throws Exception {
        // Given
        String habitEncoded ="habitId=1";

        // When & Then
        mvc.perform(post("/habitRegistration/delete")
                        .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                        .content(habitEncoded)
                        .with(csrf())
                )
                .andExpect(status().is3xxRedirection())
                .andExpect(redirectedUrlPattern("**/login"));
        then(habitRegistrationService).shouldHaveNoInteractions();
    }

    @DisplayName("[POST] 습관 신청 취소 - 인증된 사용자")
    @WithUserDetails(value = "jshTest", setupBefore = TestExecutionEvent.TEST_EXECUTION)
    @Test
    void givenHabitId_whenDeleting_thenDeletesHabitRegistration() throws Exception {
        // Given
        Long habitId = 1L;
        String userId = "jshTest";
        String habitEncoded ="habitId=1";
        willDoNothing().given(habitRegistrationService).deleteHabitRegistration(habitId,userId);

        // When & Then
        mvc.perform(post("/habitRegistration/delete")
                        .contentType(MediaType.APPLICATION_FORM_URLENCODED)
                        .content(habitEncoded)
                        .with(csrf())
                )
                .andExpect(status().is3xxRedirection())
                .andExpect(view().name("redirect:/habits/"+habitId))
                .andExpect(redirectedUrl("/habits/"+habitId));
        then(habitRegistrationService).should().deleteHabitRegistration(habitId,userId);
    }
}