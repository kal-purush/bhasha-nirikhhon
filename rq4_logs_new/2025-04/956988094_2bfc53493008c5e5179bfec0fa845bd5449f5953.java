package com.prime.constants;

/**
 * Enum representing different types of entities in the system.
 * Used for audit logging and entity type identification.
 */
public enum EntityType {
    PROJECT("PROJECT"),
    TASK("TASK");

    private final String value;

    EntityType(String value) {
        this.value = value;
    }

    public String getValue() {
        return value;
    }

    @Override
    public String toString() {
        return value;
    }
} 