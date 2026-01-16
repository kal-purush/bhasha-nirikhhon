package com.tomdaly.hotel.data.entity;

import javax.persistence.*;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

@Entity
@Table(name = "PROFANITYSET")
public class ProfanitySet {
  @Id
  @Column(name = "PROFANITYSET_ID")
  @GeneratedValue(strategy = GenerationType.IDENTITY)
  private long id;

  @Transient private Set<Profanity> profanities;

  @Column(name = "NAME")
  private final String name;

  public ProfanitySet() {
    this.name = "";
    this.profanities = new HashSet<>();
  }

  public ProfanitySet(String name) {
    this.name = name;
    this.profanities = new HashSet<>();
  }

  public long getId() {
    return id;
  }

  public void setId(long id) {
    this.id = id;
  }

  public String getName() {
    return name;
  }

  public Set<Profanity> getProfanities() {
    return profanities;
  }

  public void addProfanity(Profanity profanity) {
    profanities.add(profanity);
  }

  public void deleteProfanity(Profanity profanity) {
    profanities.remove(profanity);
  }

  public void clearProfanities() {
    profanities.clear();
  }

  @Override
  public String toString() {
    return "ProfanitySet{" + "id=" + id + ", name='" + name + "', profanities=" + profanities + '}';
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) return true;
    if (!(o instanceof ProfanitySet)) return false;
    ProfanitySet that = (ProfanitySet) o;
    return Objects.equals(name, that.name);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name);
  }
}