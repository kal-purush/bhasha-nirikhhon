package com.brainstrom.meokjang.domain;

import jakarta.persistence.*;
import lombok.Getter;
import lombok.Setter;
import lombok.ToString;

import java.util.Date;

@Entity
@Table(schema = "FOOD")
@Getter
@Setter
@ToString
public class Food {
    @Id @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long foodId;
    @Column private Long userId;
    @Column private Long infoId;
    @Column private String foodName;
    @Column private Integer stock;
    @Column private Date expireDate;
    @Column private Integer storageWay;
    @Column private Date createdAt;
}