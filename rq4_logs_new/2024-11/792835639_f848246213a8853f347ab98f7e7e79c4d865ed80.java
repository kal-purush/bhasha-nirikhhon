package com.jinius.ecommerce.user.domain.model;

import lombok.*;
import lombok.extern.slf4j.Slf4j;

import java.math.BigInteger;

@Data
@Slf4j
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@AllArgsConstructor
@Builder
public class User {
    private Long userId;
    private String name;
    private BigInteger point;

    /**
     * 포인트 충전
     * @param point
     * @return
     */
    public User addPoint(BigInteger point) {
        return User.builder()
                .userId(this.userId)
                .name(this.name)
                .point(this.point.add(point))
                .build();
    }

    /**
     * 포인트 차감
     * @param point
     * @return
     */
    public User subtractPoint(BigInteger point) {
        return User.builder()
                .userId(this.userId)
                .name(this.name)
                .point(this.point.subtract(point))
                .build();
    }
}