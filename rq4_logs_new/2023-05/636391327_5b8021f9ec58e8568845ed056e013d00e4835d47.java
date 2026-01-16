package com.codemaster.devflow.model;

import com.codemaster.devflow.model.generator.StringPrefixedSequenceIdGenerator;
import com.fasterxml.jackson.annotation.JsonProperty;
import jakarta.persistence.*;
import jakarta.validation.constraints.*;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;
import lombok.ToString;
import org.hibernate.annotations.GenericGenerator;
import org.hibernate.annotations.Parameter;

import java.io.Serializable;
import java.time.LocalDateTime;

@Data
@AllArgsConstructor
@NoArgsConstructor
@ToString
@Entity
@Table(name = "app_user")
public class User implements Serializable {
    private static final long serialVersionUID = -4330776843465387565L;

    @Id
    @GeneratedValue(strategy = GenerationType.SEQUENCE, generator = "user_seq")
    @Column(name = "userid")
    @GenericGenerator(
            name = "user_seq",
            strategy = "com.codemaster.devflow.model.generator.StringPrefixedSequenceIdGenerator",
            parameters = {
                    @Parameter(name = StringPrefixedSequenceIdGenerator.INITIAL_PARAM, value = "10000"),
                    @Parameter(name = StringPrefixedSequenceIdGenerator.INCREMENT_PARAM, value = "1"),
                    @Parameter(name = StringPrefixedSequenceIdGenerator.VALUE_PREFIX_PARAMETER, value = "usrid"),
                    @Parameter(name = StringPrefixedSequenceIdGenerator.NUMBER_FORMAT_PARAMETER, value = "%010d") })
    private String userId;


    @NotBlank(message = "First name is mandatory")
    @Size(min = 2, max = 50, message = "First name should be between 2 and 50 characters")
    @Column(name="first_name",nullable = false)
    private String firstName;

    @NotBlank(message = "Last name is mandatory")
    @Size(min = 2, max = 50, message = "Last name should be between 2 and 50 characters")
    @Column(name="last_name",nullable = false)
    private String lastName;

    @NotBlank(message = "Username cannot be blank")
    @Size(min = 4, max = 20, message = "Username must be between 4 and 20 characters")
    @Pattern(regexp = "^[a-zA-Z0-9_]+$", message = "Username must only contain letters, numbers, and underscores")
    @Column(name="user_name",nullable = false, unique = true)
    private String userName;

    @NotBlank(message = "Email cannot be blank")
    @Size(min = 15, max = 40, message = "Email must be between 15 and 40 characters")
    @Email(message = "Invalid email address")
    @Column(name="email",nullable = false,unique = true)
    private String email;

    @NotNull(message = "password cannot be null")
    @Column(name="password", nullable = false)
    private String password;

    @NotNull(message = "isActive cannot be null")
    @JsonProperty("isActive")
    @Column(name="is_active", nullable = false, columnDefinition = "NUMBER(1,0) DEFAULT 0")
    private boolean isActive;

    @NotNull(message = "isResetPassword cannot be null")
    @JsonProperty("isResetPassword")
    @Column(name="is_reset_password", nullable = false, columnDefinition = "NUMBER(1,0) DEFAULT 0")
    private boolean isResetPassword;

    @NotNull(message = "createdAt cannot be null")
    @Column(name = "created_at", nullable = false)
    private LocalDateTime createdAt;

    @NotNull(message = "updatedAt cannot be null")
    @Column(name = "updated_at", nullable = false)
    private LocalDateTime updatedAt;

    @PastOrPresent(message = "Last login must be in the past or present")
    @Column(name="last_login")
    private LocalDateTime lastLogin;

    @NotNull(message = "Last password change date cannot be null")
    @PastOrPresent(message = "Last password change date must be in the past or present")
    @Column(name="last_password_change")
    private LocalDateTime lastPasswordChange;

    @NotNull(message = "Expiration date cannot be null")
    @Future(message = "Expiration date must be in the future")
    @Column(name="expiration_date")
    private LocalDateTime expirationDate;
}