package com.lotdiz.adminservice.dto.response;

import java.time.LocalDateTime;
import lombok.*;

@Getter
@Builder
@AllArgsConstructor(access = AccessLevel.PROTECTED)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class GetProjectSearchResponseDto {

  private Long projectId;
  private String projectName;
  private String categoryName;
  private String makerName;
  private LocalDateTime projectDueDate;
  private Boolean projectIsAuthorized;
  private LocalDateTime createdAt;
}