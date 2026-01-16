package com.github.classpick.reservation.repository;

import jakarta.annotation.Nullable;
import jakarta.persistence.Entity;
import jakarta.persistence.FetchType;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.OneToOne;
import jakarta.validation.constraints.NotNull;
import lombok.AccessLevel;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;


@Getter
@Setter
@Builder
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
    private Boolean noshow;

    @NotNull
    private String ocrImage;

    public static NoshowEntity of(ReservationEntity reservation, boolean result, String key) {

        NoshowEntity entity = new NoshowEntity();
        entity.setReservation(reservation);
        entity.setNoshow(result);
        entity.setOcrImage(key);

        return entity;
    }
}