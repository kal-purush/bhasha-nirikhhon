package com.project.chamjimayo.controller.dto.request;

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Getter;
import lombok.NoArgsConstructor;

@Getter
@NoArgsConstructor
public class NicknameProfileUrlChangeRequest {
  @Schema(description ="변경할 사용자 닉네임")
  private String nickname;

  @Schema(description = "변경할 사용자 프로필 url")
  private String profileUrl;
}