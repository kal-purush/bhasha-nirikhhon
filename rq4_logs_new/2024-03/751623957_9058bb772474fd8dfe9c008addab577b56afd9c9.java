package com.example.its_a_feature_not_a_bug;

import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Map;
import java.util.stream.Collectors;

public class Organizer {
    private User organizerUser;
    private Map<String, Event> organizedEvents; // Maps event ID to Event object

    public Organizer(User user) {
        this.organizerUser = user;
        this.organizedEvents = new HashMap<>();
    }

    // Method to create a new event and generate a unique QR code
    public void createEvent(String eventName, String eventDate, String eventLocation, String eventDescription) throws EventCreationException {
        // Check if an event with the same name already exists
        if (organizedEvents.containsKey(eventName)) {
            throw new EventCreationException("Event with this name already exists.");
        }

        // Assuming the QRCodeGenerator is a class that can generate QR codes
        String eventQRCode = QRCodeGenerator.generateQRCode(eventName);
        Event event = new Event(eventName, eventDate, eventLocation, eventDescription, eventQRCode);
        organizedEvents.put(event.getEventName(), event); // Assuming eventName is unique
        // Save the event to your database here
    }

    // Method to reuse an existing QR code for a new event
    public void reuseQRCodeForEvent(String existingEventName, String newEventName, String eventDate, String eventLocation, String eventDescription) throws EventCreationException {
        Event existingEvent = organizedEvents.get(existingEventName);
        if (existingEvent != null) {
            String qrCode = existingEvent.getEventQRCode();
            Event newEvent = new Event(newEventName, eventDate, eventLocation, eventDescription, qrCode);
            organizedEvents.put(newEventName, newEvent);
            // Save the new event with the reused QR code to your database here
        } else {
            throw new EventCreationException("Existing event not found.");
        }
    }

    // Method to view the list of attendees who have checked into an event
    public List<Attendee> getCheckedInAttendees(String eventName) throws EventNotFoundException {
        Event event = organizedEvents.get(eventName);
        if (event != null) {
            // This returns a new list containing only attendees that have checked in
            return event.getAttendees().stream()
                    .filter(Attendee::isCheckedIn)
                    .collect(Collectors.toList());
        } else {
            throw new EventNotFoundException("Event not found.");
        }
    }

    // Exception classes
    public static class EventCreationException extends Exception {
        public EventCreationException(String message) {
            super(message);
        }
    }

    public static class EventNotFoundException extends Exception {
        public EventNotFoundException(String message) {
            super(message);
        }
    }

    // Getters and Setters
    public User getOrganizerUser() {
        return organizerUser;
    }

    public void setOrganizerUser(User organizerUser) {
        this.organizerUser = organizerUser;
    }

    public Map<String, Event> getOrganizedEvents() {
        return organizedEvents;
    }

    public void setOrganizedEvents(Map<String, Event> organizedEvents) {
        this.organizedEvents = organizedEvents;
    }
}