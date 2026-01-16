package com.example.swiftgathering_server.domain;

import jakarta.persistence.Column;
import jakarta.persistence.Embeddable;
import lombok.NoArgsConstructor;
import lombok.Setter;

import java.io.Serializable;
import java.util.Objects;

@Embeddable
@NoArgsConstructor
@Setter
public class FriendshipId implements Serializable {

    @Column(name = "younger_member_id")
    private Long youngerMemberId;

    @Column(name = "older_member_id")
    private Long olderMemberId;

    public FriendshipId(Long youngerMemberId, Long olderMemberId) {
        this.youngerMemberId = youngerMemberId;
        this.olderMemberId = olderMemberId;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        FriendshipId friendship = (FriendshipId) o;
        return Objects.equals(youngerMemberId, friendship.youngerMemberId) && Objects.equals(olderMemberId, friendship.olderMemberId);
    }

    @Override
    public int hashCode() {
        return Objects.hash(youngerMemberId, olderMemberId);
    }
}