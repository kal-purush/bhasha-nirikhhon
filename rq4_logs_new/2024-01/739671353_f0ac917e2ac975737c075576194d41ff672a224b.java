package com.fpr.user.entity;

import jakarta.persistence.*;
import lombok.Data;
import org.springframework.data.annotation.Persistent;

import java.time.LocalDate;

@Entity
@Data
@Table(name = "user_financial_goal")
public class User {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long UserId;

    private String nickname;

    private String email;

    private LocalDate createDate;

    @PrePersist
    private void prePersist(){
        this.createDate = LocalDate.now();
    }

}