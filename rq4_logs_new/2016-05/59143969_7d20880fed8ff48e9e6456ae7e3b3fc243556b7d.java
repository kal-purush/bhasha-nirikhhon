package com.bls.patronage.api;

import java.util.Date;
import java.util.UUID;

abstract public class AuditableRepresentation {
    private Date createdAt;
    private Date modifiedAt;
    private UUID createdBy;
    private UUID modifiedBy;

    public Date getCreatedAt() {
        return createdAt;
    }

    public Date getModifiedAt() {
        return modifiedAt;
    }

    public UUID getCreatedBy() {
        return createdBy;
    }

    public UUID getModifiedBy() {
        return modifiedBy;
    }

    public void setCreatedAt(Date createdAt) {
        this.createdAt = createdAt;
    }

    public void setModifiedAt(Date modifiedAt) {
        this.modifiedAt = modifiedAt;
    }

    public void setCreatedBy(UUID createdBy) {
        this.createdBy = createdBy;
    }

    public void setModifiedBy(UUID modifiedBy) {
        this.modifiedBy = modifiedBy;
    }
}