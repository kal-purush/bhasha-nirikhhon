package com.todolist.models;

import java.time.Instant;

public interface Auditable {
    Instant getCreatedAt();
    Instant getLastUpdatedAt();

}