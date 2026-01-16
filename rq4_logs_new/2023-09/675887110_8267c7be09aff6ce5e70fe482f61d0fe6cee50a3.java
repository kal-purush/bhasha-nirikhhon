package library;

/**
 * Class that represents an event happening in a library;
 *
 * @author Matheus Reato (RA: 244088), Caio Taishi (RA: 242908).
 */

import java.time.LocalDate;
import java.time.LocalDateTime;

public class Event {
    // Enums ----------------------------------------------------------------------------
    public enum EventType {
        TALK, WORKSHOP, EXHIBIT
    }

    // **********************************************************************************
    public static class Talk {
        // Private Attributes ***************************************
        private final EventType type;
        private final String lecturer;
        private final String topic;
        private LocalDateTime dateTime;
        private String place;

        // Constructor **********************************************
        public Talk(String lecturer, String topic, LocalDateTime dateTime, String place) {
            this.type = EventType.TALK;
            this.lecturer = lecturer;
            this.topic = topic;
            this.dateTime = dateTime;
            this.place = place;
        }

        // Getters **************************************************
        public String getLecturer() {
            return lecturer;
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

        // Setters **************************************************
        private void setDateTime(LocalDateTime newDateTime) {
            this.dateTime = newDateTime;
        }

        private void setPlace(String newPlace) {
            this.place = newPlace;
        }
    }

    // **********************************************************************************
    public static class Workshop {
        // Private Attributes ***************************************
        private final EventType type;
        private final String instructor;
        private final String[] materials;
        private LocalDateTime dateTime;
        private String place;

        // Constructor **********************************************
        public Workshop(String instructor, String[] materials, LocalDateTime dateTime,
                        String place) {
            this.type = EventType.WORKSHOP;
            this.instructor = instructor;
            this.materials = materials;
            this.dateTime = dateTime;
            this.place = place;
        }

        // Getters **************************************************
        public EventType getType() {
            return type;
        }

        public String getInstructor() {
            return instructor;
        }

        public String[] getMaterials() {
            return materials;
        }

        public LocalDateTime getDateTime() {
            return dateTime;
        }

        public String getPlace() {
            return place;
        }

        // Setters **************************************************
        private void setDateTime(LocalDateTime newDateTime) {
            this.dateTime = newDateTime;
        }

        private void setPlace(String newPlace) {
            this.place = newPlace;
        }
    }

    // **********************************************************************************
    public static class Exhibit {
        // Private Attributes ***************************************
        private final EventType type;
        private final String[] exhibitors;
        private final String topic;
        private LocalDateTime startDateTime;
        private String duration;
        private String place;

        // Constructor **********************************************
        public Exhibit(String[] exhibitors, String topic, LocalDateTime startDateTime,
                       String duration, String place) {
            this.type = EventType.EXHIBIT;
            this.exhibitors = exhibitors;
            this.topic = topic;
            this.startDateTime = startDateTime;
            this.duration = duration;
            this.place = place;
        }

        // Getters **************************************************
        public EventType getType() {
            return type;
        }

        public String[] getExhibitors() {
            return exhibitors;
        }

        public String getTopic() {
            return topic;
        }

        public LocalDateTime getStartDateTime() {
            return startDateTime;
        }

        public String getDuration() {
            return duration;
        }

        public String getPlace() {
            return place;
        }

        // Setters **************************************************
        private void setStartDateTime(LocalDateTime newStartDateTime) {
            this.startDateTime = newStartDateTime;
        }

        private void setDuration(String newDuration) {
            this.duration = newDuration;
        }

        private void setPlace(String newPlace) {
            this.place = place;
        }
    }
}