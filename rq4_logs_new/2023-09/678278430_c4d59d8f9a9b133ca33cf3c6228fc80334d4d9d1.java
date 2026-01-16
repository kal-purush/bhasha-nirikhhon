package com.openlabs.shoppingmall.entity;

import lombok.*;

import javax.persistence.*;
import java.time.LocalDateTime;

@Table(name = "USERCOUPON")
@Entity
@Builder
@Getter
@AllArgsConstructor
@NoArgsConstructor(access = AccessLevel.PROTECTED)
public class UserCoupons extends BaseEntity {
    /** 고객쿠폰ID */
    @Id @GeneratedValue
    @Column(name = "USERCOUPON_ID")
    private Long usercouponId;
    /** 사용일자 */
    @Column(name = "USE_DATE")
    private LocalDateTime useDate;
    /** 사용여부 */
    @Column(name = "USE_YN")
    private Long useYn;

    /** 고객DTO */
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "USER_ID")
    private Users users;
    /** 쿠폰DTO */
    @ManyToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "COUPON_ID")
    private Coupons coupons;
}