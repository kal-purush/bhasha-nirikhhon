package com.flexiorder.application.model;

import com.flexiorder.application.enums.RoleEnum;
import jakarta.persistence.*;
import jakarta.validation.constraints.Email;
import jakarta.validation.constraints.NotBlank;
import jakarta.validation.constraints.NotNull;
import jakarta.validation.constraints.Size;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;
import org.hibernate.annotations.CreationTimestamp;
import org.hibernate.annotations.GenericGenerator;

import javax.management.relation.Role;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;

@Getter
@Setter
@NoArgsConstructor
@Entity
@Table(name = "users",
    uniqueConstraints = {
        @UniqueConstraint(name = "UC_USER__EMAIL", columnNames = "email"),
        @UniqueConstraint(name = "UC_USER__PHONE_NUMBER", columnNames = "phoneNumber")
    })
public class User {

    @Id
    @Column(columnDefinition = "uuid")
    @GeneratedValue(generator = "uuid")
    @GenericGenerator(name = "uuid")
    @NotNull
    protected UUID id;

    @CreationTimestamp
    private Instant creationDate;

    @CreationTimestamp
    private Instant lastUpdateDate;

    @NotBlank
    @Size(max = 120)
    private String name;

    @NotBlank
    @Size(max = 60)
    @Email
    private String email;

    @Size(max = 14)
    private String phoneNumber;

    private String document;

    @NotBlank
    @Size(max = 120)
    private String password;

    @ElementCollection(targetClass = RoleEnum.class)
    @CollectionTable(name = "user_roles", joinColumns = @JoinColumn(name = "user_id"),
            foreignKey = @ForeignKey(name = "FK_USER_ROLES__USER_ID"))
    private List<RoleEnum> roles = new ArrayList<>();

    public User(String name, String email, String phoneNumber, String password, Boolean temporaryPassword, Boolean enableShortcutWallet, Boolean enableShortcutDashboard) {
        this.name = name;
        this.email = email;
        this.phoneNumber = phoneNumber;
        this.password = password;
    }

    public User(String name, String email) {
        this.name = name;
        this.email = email;
    }

    public User(UUID loggedUserId) {
        this.id = loggedUserId;
    }

}