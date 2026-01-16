package com.self.egoboard.domain.user.api;

import static org.springframework.restdocs.mockmvc.RestDocumentationRequestBuilders.post;

import com.self.egoboard.IntegrationTest;
import com.self.egoboard.domain.user.dto.request.UserSignUpReqDto;
import com.self.egoboard.domain.user.entity.User;
import com.self.egoboard.domain.user.entity.embedded.Address;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.CsvSource;
import org.junit.jupiter.params.provider.ValueSource;
import org.springframework.http.MediaType;
import org.springframework.test.web.servlet.ResultMatcher;
import org.springframework.test.web.servlet.result.MockMvcResultMatchers;

class UserControllerTest extends IntegrationTest {

  @Nested
  @DisplayName("유저 생성")
  class createUser {

    private User user;

    @BeforeEach
    void setUp() {
      user = userTestHelper.generate();
    }

    private void mockMvcTest(UserSignUpReqDto userSignUpReqDto, ResultMatcher Created)
        throws Exception {
      mockMvc.perform(post("/api/v1/user/sign-up")
              .contentType(MediaType.APPLICATION_JSON)
              .content(objectMapper.writeValueAsString(userSignUpReqDto)))
          .andExpect(Created);
    }

    @Test
    @DisplayName("유저 생성에 성공한다.")
    void should_201CREATE_when_createUser() throws Exception {

      UserSignUpReqDto userSignUpReqDto = new UserSignUpReqDto(
          user.getEmailAddress(),
          user.getPassword(),
          user.getUsername(),
          user.getNickname(),
          user.getAddress()
      );

      mockMvcTest(userSignUpReqDto, MockMvcResultMatchers.status().isCreated());
    }


    @ParameterizedTest
    @ValueSource(strings = {"Hello", "JUnit@", "@Parameterized", "1234"})
    @DisplayName("이메일 형식이 다르면, 유저 생성에 실패한다.")
    void should_400BadRequest_when_diffEmail(String param) throws Exception {
      UserSignUpReqDto userSignUpReqDto = new UserSignUpReqDto(
          param,
          user.getPassword(),
          user.getUsername(),
          user.getNickname(),
          user.getAddress()
      );

      mockMvcTest(userSignUpReqDto, MockMvcResultMatchers.status().isBadRequest());
    }

    @ParameterizedTest
    @ValueSource(strings = {"Hello", "JUnit@", "@Parameterized", "1234", "abcdefghizk"})
    @DisplayName("페스워드 형식이 다르면, 유저 생성에 실패한다.")
    void should_400BadRequest_when_diffPassword(String param) throws Exception {
      UserSignUpReqDto userSignUpReqDto = new UserSignUpReqDto(
          user.getEmailAddress(),
          param,
          user.getUsername(),
          user.getNickname(),
          user.getAddress()
      );

      mockMvcTest(userSignUpReqDto, MockMvcResultMatchers.status().isBadRequest());
    }

    @ParameterizedTest
    @ValueSource(strings = {"", "abcdefghizkzxcvasdf"})
    @DisplayName("페스워드 형식이 다르면, 유저 생성에 실패한다.")
    void should_400BadRequest_when_diffNickname(String param) throws Exception {
      UserSignUpReqDto userSignUpReqDto = new UserSignUpReqDto(
          user.getEmailAddress(),
          user.getPassword(),
          user.getUsername(),
          param,
          user.getAddress()
      );

      mockMvcTest(userSignUpReqDto, MockMvcResultMatchers.status().isBadRequest());
    }

    @ParameterizedTest
    @CsvSource({"as, abcdefghizkzxcvasdf, 12345", "121, street12, 123456", "city121, 123, 123456"})
    @DisplayName("페스워드 형식이 다르면, 유저 생성에 실패한다.")
    void should_400BadRequest_when_diffAddress(String city, String street, String zipcode)
        throws Exception {
      UserSignUpReqDto userSignUpReqDto = new UserSignUpReqDto(
          user.getEmailAddress(),
          user.getPassword(),
          user.getUsername(),
          user.getNickname(),
          Address.builder()
              .city(city)
              .street(street)
              .zipcode(zipcode)
              .build()
      );

      mockMvcTest(userSignUpReqDto, MockMvcResultMatchers.status().isBadRequest());
    }
  }
}