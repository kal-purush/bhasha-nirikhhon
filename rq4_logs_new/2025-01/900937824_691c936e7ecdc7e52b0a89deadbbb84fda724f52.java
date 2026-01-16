package ru.yandex.practicum.filmorate.model;

import lombok.Data;

@Data
public class UserEvent {
    private long eventId;
    private long userId;
    private long entityId;
    private EventType eventType;
    private EventOperation operation;
    private long timestamp;

    public enum EventOperation {
        REMOVE,
        ADD,
        UPDATE
    }

    public enum EventType {
        LIKE,
        REVIEW,
        FRIEND
    }
}