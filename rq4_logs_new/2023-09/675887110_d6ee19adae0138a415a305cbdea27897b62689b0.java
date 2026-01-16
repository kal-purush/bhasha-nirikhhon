package library.management;

import library.Library;
import people.Person;

import java.time.LocalDate;
import java.util.ArrayList;

/**
 * Class that represents an event happening at a library.
 *
 * @author Matheus Reato (RA: 244088), Caio Taishi (RA: 242908)
 */

public class LibraryEvent {
    // Private Attributes ---------------------------------------------------------------
    private Library library;
    private LocalDate date;
    private String topic;
    private EventType eventType;
    private int maxAttendants;
    private int numOfAttendants;
    private ArrayList<people.Person> attendants;
    // Event room will go here.
    private EventStatus eventStatus;

    // Constructor ----------------------------------------------------------------------
    public LibraryEvent(Library library, LocalDate date, String topic, EventType eventType,
                        int maxAttendants) {
        this.library = library;
        this.date = date;
        this.topic = topic;
        this.eventType = eventType;
        this.maxAttendants = maxAttendants;
        this.numOfAttendants = 0;
        this.attendants = new ArrayList<>();
        this.eventStatus = EventStatus.PENDING;
    }

    // Inner Classes --------------------------------------------------------------------
    // Lecture Inner Class **************************************************************
    public class Lecture {
        // Private Attributes ***************************************
        private String lecturer;                   // Lecturer class?

        // Constructor **********************************************
        public Lecture(String lecturer) {
            this.lecturer = lecturer;
        }

        // Getters **************************************************
        public String getLecturer() {
            return lecturer;
        }

        // Setters **************************************************
        private void setLecturer(String newLecturer) {
            this.lecturer = newLecturer;
        }
    }

    // Workshop Inner class *************************************************************
    public class Workshop {
        // Private Attributes ***************************************
        private String instructor;               // Instructor class?
        private ArrayList<String> materials;

        // Constructor **********************************************
        public Workshop(String instructor) {
            this.instructor = instructor;
            this.materials = new ArrayList<>();
        }

        // Getters **************************************************
        public String getInstructor() {
            return instructor;
        }

        public ArrayList<String> getMaterials() {
            return materials;
        }

        // Setters **************************************************
        private void setInstructor(String newInstructor) {
            this.instructor = newInstructor;
        }

        // Methods **************************************************

        /**
         * Method that adds a material to the workshop's material list
         *
         * @param newMaterial the material to be added.
         */
        public void addMaterial(String newMaterial) {
            (this.materials).add(newMaterial);
        }
    }

    // Exhibit class ********************************************************************
    public class Exhibit {
        // Private Attributes ***************************************
        private ArrayList<String> exhibitors;

        // Constructor **********************************************
        public Exhibit() {
            this.exhibitors = new ArrayList<>();
        }

        // Getters **************************************************
        public ArrayList<String> getExhibitors() {
            return exhibitors;
        }
    }

    // Getters --------------------------------------------------------------------------
    public Library getLibrary() {
        return library;
    }

    public LocalDate getDate() {
        return date;
    }

    public String getTopic() {
        return topic;
    }

    public EventType getEventType() {
        return eventType;
    }

    public int getMaxAttendants() {
        return maxAttendants;
    }

    public int getNumOfAttendants() {
        return numOfAttendants;
    }

    public ArrayList<Person> getAttendants() {
        return attendants;
    }

    public EventStatus getEventStatus() {
        return eventStatus;
    }

    // Setters --------------------------------------------------------------------------
    private void setLibrary(Library newLibrary) {
        this.library = newLibrary;
    }

    private void setDate(LocalDate newDate) {
        this.date = newDate;
    }

    private void setTopic(String newTopic) {
        this.topic = newTopic;
    }

    private void setEventType(EventType newEventType) {
        this.eventType = newEventType;
    }

    private void setMaxAttendants(int newMaxAttendants) {
        this.maxAttendants = newMaxAttendants;
    }

    private void setNumOfAttendants(int newNumOfAttendants) {
        this.numOfAttendants = newNumOfAttendants;
    }

    public void setEventStatus(EventStatus newEventStatus) {
        this.eventStatus = newEventStatus;
    }

    // Methods --------------------------------------------------------------------------

    /**
     * Method to update the amount of people attending the event
     */
    public void updateNumOfAttendants() {
        this.setNumOfAttendants(this.getAttendants().size());
    }

    /**
     * Method to try and add a new person to the list of people who will be attending
     * the event.
     *
     * @param attendant the person to be added.
     * @return true on success and false on failure.
     */
    public boolean addAttendant(people.Person attendant) {
        if (this.getNumOfAttendants() < this.getMaxAttendants()) {
            (this.attendants).add(attendant);
            return true;
        }
        else {
            System.out.println("Sorry, the event is full.");
            return false;
        }
    }
}