package com.example.cargo_transportation.entity;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import lombok.*;

import java.io.Serializable;
import java.util.Objects;

@Embeddable
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
public class RenderFavorId implements Serializable {
    @Column
    private Long journalId;
    @Column
    private Long favorId;

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        RenderFavorId that = (RenderFavorId) o;
        return Objects.equals(journalId, that.journalId) && Objects.equals(favorId, that.favorId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(journalId, favorId);
    }
}