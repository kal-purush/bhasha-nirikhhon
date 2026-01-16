package com.lotdiz.adminservice.dto.response;

import java.time.LocalDateTime;
import lombok.*;

@Getter
@Builder
@AllArgsConstructor(access = AccessLevel.PROTECTED)
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class GetMakerSearchResponseDto {

    private Long makerId;
    private String makerName;
    private String makerEmail;
    private String makerPhoneNumber;
    private String makerKakaoUrl;
    private String makerHomeUrl;
    private String makerSnsUrl;
    private LocalDateTime createdAt;
}