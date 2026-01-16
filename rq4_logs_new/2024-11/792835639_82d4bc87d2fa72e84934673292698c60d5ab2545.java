package com.jinius.ecommerce.user.api.dto;

import com.jinius.ecommerce.user.application.dto.UserPointDto;
import com.jinius.ecommerce.user.domain.User;
import com.jinius.ecommerce.user.domain.UserPointService;
import lombok.*;

import java.math.BigInteger;

@Getter
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor
@Builder
public class UserPointResponse {
    private Long userId;
    private String userName;
    private BigInteger point;

    public static UserPointResponse from(UserPointDto dto) {
        return UserPointResponse.builder()
                .userId(dto.getUserId())
                .userName(dto.getUserName())
                .point(dto.getPoint())
                .build();
    }
}