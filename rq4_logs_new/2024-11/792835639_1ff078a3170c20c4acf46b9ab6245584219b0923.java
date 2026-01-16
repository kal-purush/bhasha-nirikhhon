package com.jinius.ecommerce.user.domain.model;

import lombok.*;
import lombok.extern.slf4j.Slf4j;

import java.math.BigInteger;

@Data
@Slf4j
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor
@Builder
public class UpdateUser {
    private Long userId;
    private String name;
    private BigInteger point;
    private int originVersion;          //동시성 제어(낙관적 락)을 위한 버전 추가
    private int newVersion;             //동시성 제어(낙관적 락)을 위한 버전 추가

    public static UpdateUser toChargeUser(User user, BigInteger point) {
        return UpdateUser.builder()
                .userId(user.getUserId())
                .name(user.getName())
                .point(user.getPoint().add(point))
                .originVersion(user.getVersion())
                .newVersion(user.getVersion()+1)
                .build();
    }

    public static UpdateUser toUseUser(User user, BigInteger point) {
        return UpdateUser.builder()
                .userId(user.getUserId())
                .name(user.getName())
                .point(user.getPoint().subtract(point))
                .originVersion(user.getVersion())
                .newVersion(user.getVersion()+1)
                .build();
    }

}