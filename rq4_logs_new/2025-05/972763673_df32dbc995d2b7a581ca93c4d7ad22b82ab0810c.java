package com.github.classpick.noshow.repository;

import com.github.classpick.reservation.repository.ReservationEntity;
import jakarta.annotation.Nullable;
import jakarta.persistence.*;
import jakarta.validation.constraints.NotNull;
import lombok.*;


@Getter
@Setter
@AllArgsConstructor
@NoArgsConstructor(access = AccessLevel.PROTECTED)
@Entity
public class NoshowEntity {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long noshowId;

    @OneToOne(fetch = FetchType.LAZY)
    @JoinColumn(name = "reservation_id")
    private ReservationEntity reservation;

    @Nullable
    private boolean certification;

    @NotNull
    private String ocrImage;

}