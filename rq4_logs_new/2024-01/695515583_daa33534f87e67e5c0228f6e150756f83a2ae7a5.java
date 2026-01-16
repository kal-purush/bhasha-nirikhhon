package com.joboffers.apivalidationerror;

import com.joboffers.BaseIntegrationTest;
import com.joboffers.infrastructure.apivalidation.ApiValidateErrorDto;
import org.junit.jupiter.api.Test;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.ResultActions;

import static org.assertj.core.api.Assertions.assertThat;
import static org.springframework.test.web.servlet.request.MockMvcRequestBuilders.post;
import static org.springframework.test.web.servlet.result.MockMvcResultMatchers.status;

public class ApiValidationFailedIntegrationTest extends BaseIntegrationTest {

    @Test
    public void should_return_400_bad_request_and_validation_message_when_all_is_null_in_offer_save_request()throws Exception{

        // given & then
        ResultActions performPostWithNulls = mockMvc.perform(post("/offers")
                .content("""
                        {
                        }
                        """.trim())
                .contentType(MediaType.APPLICATION_JSON)
        );

        String json = performPostWithNulls.andExpect(status().isBadRequest())
                .andReturn()
                .getResponse()
                .getContentAsString();

        ApiValidateErrorDto result = objectMapper.readValue(json,ApiValidateErrorDto.class);
        assertThat(result.message()).containsExactlyInAnyOrder(
                "companyName.not.blank",
                "companyName.not.null",
                "salary.not.blank",
                "url.not.null",
                "position.not.null",
                "url.not.blank",
                "salary.not.null",
                "position.not.blank"
        );

    }


}