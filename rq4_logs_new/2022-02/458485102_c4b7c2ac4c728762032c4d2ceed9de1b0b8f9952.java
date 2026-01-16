package org.example.projectapp.service.exception;

public class CustomEntityNotFoundException extends RuntimeException {
    private final Long entityId;
    private final String className;


    public CustomEntityNotFoundException(Long entityId, String className) {
        super(String.format("Entity %s not found with id %s", className, entityId));
        this.entityId = entityId;
        this.className = className;
    }

    public Long getEntityId() {
        return entityId;
    }

    public String getClassName() {
        return className;
    }
}