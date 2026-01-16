package com.self.egoboard.domain.user.entity;

import static jakarta.persistence.GenerationType.IDENTITY;

import com.self.egoboard.domain.user.entity.embedded.Address;
import com.self.egoboard.global.Entity.BaseEntity;
import jakarta.persistence.Column;
import jakarta.persistence.Embedded;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.Builder;
import lombok.NoArgsConstructor;

@Entity
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@Table(name = "user")
public class User extends BaseEntity {

  private static final int MAX_PASSWORD_LENGTH = 15;
  private static final int MAX_EMAIL_ADDRESS_LENGTH = 15;
  private static final int MAX_USERNAME_LENGTH = 15;
  private static final int MAX_NICKNAME_LENGTH = 15;

  @Id
  @GeneratedValue(strategy = IDENTITY)
  @Column(name = "user_id", nullable = false, updatable = false)
  private Long id;

  @Column(name = "username", nullable = false, length = MAX_USERNAME_LENGTH, updatable = false)
  private String username;

  @Column(name = "money", nullable = false, length = MAX_PASSWORD_LENGTH)
  private String password;

  @Column(name = "email_address", nullable = false, length = MAX_EMAIL_ADDRESS_LENGTH)
  private String emailAddress;

  @Column(name = "nickname", nullable = false, length = MAX_NICKNAME_LENGTH)
  private String nickname;

  @Embedded
  private Address address;

}