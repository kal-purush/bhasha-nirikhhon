package com.openlabs.shoppingmall.dto;

import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.time.LocalDateTime;

@Slf4j
@Data
@Builder
@NoArgsConstructor
@AllArgsConstructor
public class UserCouponsReqDto {
    /** 고객쿠폰ID */
    private Long usercouponId;
    /** 사용일자 */
    private LocalDateTime useDate;
    /** 사용여부 */
    private String useYn;
    /** 고객ID */
    private String userId;
    /** 쿠폰ID */
    private Long couponId;
}