package com.jeontongju.product.dto.request;

import javax.validation.constraints.Size;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;

@AllArgsConstructor
@NoArgsConstructor
@Getter
@Builder
public class UpdateShortsDto {

  @Size(min = 1, max = 30, message = "내용은 1글자 이상 30 글자 이하만 입력 가능합니다.")
  private String shortsTitle;

  @Size(min = 1, max = 300, message = "내용은 1글자 이상 300 글자 이하만 입력 가능합니다.")
  private String shortsDescription;

  private Boolean isActivate;
}