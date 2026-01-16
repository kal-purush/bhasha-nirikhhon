package com.marketplace.domain;

import java.util.UUID;

public record ClassifiedApproved(UUID id, UserId value) {
    public ClassifiedApproved {
        if (value == null) {
            throw new IllegalArgumentException("value cannot be null");
        }
    }

    public static ClassifiedApproved fromString(ClassifiedAdId id, String value) {
        return new ClassifiedApproved(id.id(), UserId.fromString(value));
    }

    @Override
    public String toString() {
        return value.toString();
    }
}