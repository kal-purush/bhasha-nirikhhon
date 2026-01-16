package com.example.oneulnail.domain.shop.entity;

import com.example.oneulnail.global.entity.BaseEntity;
import lombok.Builder;
import lombok.Getter;

import javax.persistence.*;

@Getter
@Entity
@Table(name = "shop")
public class Shop extends BaseEntity {

    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    private String name;

    @Column(length = 11)
    private String phoneNum;

    private String location;    // 지역

    @Column(name = "operating_hours")
    private String operatingHours;  // 영업시간

    private int likeCnt;    // 즐겨찾기 개수

    @Column(name = "img_url")
    private String imgUrl;

    private int price;

    @Column(name = "shop_info")
    private String shopInfo;
}