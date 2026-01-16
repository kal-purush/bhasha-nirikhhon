package com.example.auction.Coupon;

import com.example.auction.Coupon.Dto.CouponRequestDto;
import com.example.auction.Global.BaseEntity;
import jakarta.persistence.*;
import lombok.Getter;

import java.time.LocalDateTime;

@Entity
@Getter
@Table(name = "coupon")
public class Coupon extends BaseEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(nullable = false)
    private Long userId;

    @Column(nullable = false)
    private String name;

    private String image;

    @Column(nullable = false)
    private int amount;

    @Column(nullable = false)
    private int discountAmount;

    @Column(nullable = false)
    private CouponStatus status = CouponStatus.AVAILABLE;

    @Column(nullable = false)
    private LocalDateTime expiredAt;

    public Coupon(){}

    public Coupon(Long userId , CouponRequestDto requestDto){
        this.userId = userId;
        this.image = requestDto.getImage();
        this.name = requestDto.getName();
        this.amount = requestDto.getAmount();
        this.discountAmount = requestDto.getDiscountAmount();
    }

    public void setExpiredAt(LocalDateTime expiredAt){
        this.expiredAt = expiredAt;
    }
}