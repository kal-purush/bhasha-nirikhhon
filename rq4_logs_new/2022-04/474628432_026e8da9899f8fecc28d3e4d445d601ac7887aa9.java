package com.example.registerstartupproject.entity;

import lombok.*;
import org.hibernate.Hibernate;

import javax.persistence.*;
import java.time.LocalDateTime;
import java.util.Objects;

@Entity
@Getter
@Setter
@ToString
@RequiredArgsConstructor
@NoArgsConstructor
@AllArgsConstructor
public class RegisterIdea {
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Id
   private Long id;
   private String review;
   private LocalDateTime reviewSendingDate;
    @Enumerated(EnumType.ORDINAL)
   private Status status;
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || Hibernate.getClass(this) != Hibernate.getClass(o)) return false;
        RegisterIdea that = (RegisterIdea) o;
        return id != null && Objects.equals(id, that.id);
    }
    @Override
    public int hashCode() {
        return getClass().hashCode();
    }
}