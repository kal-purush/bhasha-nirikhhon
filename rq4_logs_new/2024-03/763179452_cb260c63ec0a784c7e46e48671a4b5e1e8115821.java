package com.BackendChallenge.TechTrendEmporium.entity;

import jakarta.persistence.*;
import lombok.Data;

import java.util.Set;


@Data
@Entity
@Table(name = "cart") // Table name: cart
public class Cart {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id; // id to uniquely identify each cart

    @OneToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "user_id")
    private User user; // user_id: connect with the user table

    @OneToMany(mappedBy = "cart", cascade = CascadeType.ALL)
    private Set<CouponCart> couponCart; // Connects with the coupon_cart table

    // Other attributes as needed
}