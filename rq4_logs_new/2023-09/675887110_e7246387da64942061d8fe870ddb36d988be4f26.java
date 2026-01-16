package models.library.management;

import java.time.LocalDateTime;

public abstract class LibraryEvent {
    // Private Attributes ---------------------------------------------------------------
    private final EventType type;
    private final String topic;
    private LocalDateTime dateTime;
    private String place;

    // Constructor ----------------------------------------------------------------------
    public LibraryEvent(EventType type, String topic, LocalDateTime dateTime, String place) {
        this.type = type;
        this.topic = topic;
        this.dateTime = dateTime;
        this.place = place;
    }

    // Enums ----------------------------------------------------------------------------
    public enum EventType {
        TALK, WORKSHOP, EXHIBIT
    }

    // Getters --------------------------------------------------------------------------
    public EventType getType() {
        return type;
    }

    public String getTopic() {
        return topic;
    }

    public LocalDateTime getDateTime() {
        return dateTime;
    }

    public String getPlace() {
        return place;
    }

    // Setters --------------------------------------------------------------------------
    private void setDateTime(LocalDateTime dateTime) {
        this.dateTime = dateTime;
    }

    private void setPlace(String place) {
        this.place = place;
    }
}