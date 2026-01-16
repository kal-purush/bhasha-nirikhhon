package com.marketplace.domain.userprofile;

import com.marketplace.framework.Strings;

public record FullName(String firstName, String middleName, String lastName) {
    public FullName {
        if (Strings.isNullOrEmpty(firstName)) {
            throw new IllegalArgumentException("first name cannot be null or empty");
        }
        if (Strings.isNullOrEmpty(lastName)) {
            throw new IllegalArgumentException("last name cannot be null or empty");
        }
    }

    public String fullName() {
        StringBuilder b = new StringBuilder();
        b.append(firstName);
        if (!Strings.isNullOrEmpty(middleName)) {
            b.append(" ").append(middleName);
        }
        b.append(" ").append(lastName);
        return b.toString();
    }

    @Override
    public String toString() {
        return fullName();
    }
}