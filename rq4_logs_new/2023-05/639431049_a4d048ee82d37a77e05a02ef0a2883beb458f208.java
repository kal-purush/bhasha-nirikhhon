package com.self.egoboard.domain.user.dto.request;

import com.self.egoboard.domain.user.entity.embedded.Address;
import jakarta.validation.Valid;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Pattern;
import lombok.Builder;

public record UserSignUpReqDto(

    @NotNull(message = NULL_MESSAGE) @Email
    String emailAddress,
    @NotNull(message = NULL_MESSAGE) @Pattern(regexp = "^(?=.*?[A-Za-z])(?=.*?\\\\d).{8,20}$", message = PASSWORD_INVALID)
    String password,
    @NotNull(message = NULL_MESSAGE) @Pattern(regexp = "^[a-zA-Z가-힣]{1,20}", message = USER_NAME_INVALID)
    String username,
    @NotNull(message = NULL_MESSAGE) @Pattern(regexp = "^[a-zA-Z0-9가-힣ㄱ-ㅎㅏ-ㅣ]{1,16}", message = NICKNAME_INVALID)
    String nickname,
    @Valid Address address
) {

  public static final String PASSWORD_INVALID = "비밀번호는 8~20자여야 하고 영어, 숫자가 포함되어야 합니다.";
  public static final String USER_NAME_INVALID = "이름은 1~20자 한글, 영어만 가능합니다.";
  public static final String NICKNAME_INVALID = "닉네임은 1~16자 한글, 영어, 숫자만 가능합니다.";
  public static final String NULL_MESSAGE = "입력되지 않은 값이 있습니다.";


}