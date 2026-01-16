package com.self.egoboard.domain.pet.entity;

import com.self.egoboard.domain.user.entity.User;
import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.OneToOne;
import jakarta.persistence.Table;
import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@Entity
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@Table(name = "pet")
public class Pet {

  @Id
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  @Column(name = "pet_id", nullable = false, updatable = false)
  private Long id;

  @Column(name = "level", nullable = false)
  private Integer level;

  @Column(name = "exp", nullable = false)
  private Integer exp;

  @Enumerated
  private PetType petType;

  @OneToOne(mappedBy = "pet", fetch = FetchType.LAZY)
  private User user;
}