package com.reddot.app.entity;

import com.reddot.app.entity.enumeration.MembershipRank;
import jakarta.persistence.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.List;

@Data
@AllArgsConstructor
@NoArgsConstructor
@Entity(name = "membership")
public class Membership {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Enumerated(EnumType.STRING)
    @Column(name = "membership_rank" , nullable = false)
    private MembershipRank rank;

    private LocalDateTime startDate;
    private LocalDateTime endDate;

    private boolean isActive;

    @OneToOne
    @JoinColumn(name = "user_id",nullable = false , unique = true) // Ensures that each user can only have one active membership
    private User user;
}