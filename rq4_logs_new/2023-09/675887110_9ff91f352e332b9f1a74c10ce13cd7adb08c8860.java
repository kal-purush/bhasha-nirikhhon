package library;

import library.Library;

/**
 * Class that represents a room in the library.
 *
 * @author Matehus Reato (RA: 244088), Caio Taishi (RA: 242908).
 */

public class Room {
    // Private Attributes ---------------------------------------------------------------
    private final Library library;
    private final String roomNumber;
    private final RoomType type;
    private final int maxPeople;
    private boolean available;
    private final boolean hasComputer;
    private final boolean hasMultimedia;

    // Constructor ----------------------------------------------------------------------
    public Room(Library library, String roomNumber, RoomType type, int maxPeople,
                boolean hasComputer, boolean hasMultimedia) {
        this.library = library;
        this.roomNumber = roomNumber;
        this.type = type;
        this.maxPeople = maxPeople;
        this.available = true;
        this.hasComputer = hasComputer;
        this.hasMultimedia = hasMultimedia;
    }

    public Room(Library library, String roomNumber, RoomType type, boolean available,
                boolean hasComputer, boolean hasMultimedia) {
        this.library = library;
        this.roomNumber = roomNumber;
        this.type = type;
        this.maxPeople = 1;               // Individual room (maxPeople always equals 1).
        this. available = available;
        this.hasComputer = hasComputer;
        this.hasMultimedia = hasMultimedia;
    }

    public Room(Library library, String roomNumber, RoomType type, int maxPeople, boolean available,
                boolean hasComputer, boolean hasMultimedia) {
        this.library = library;
        this.roomNumber = roomNumber;
        this.type = type;
        this.maxPeople = maxPeople;
        this. available = available;
        this.hasComputer = hasComputer;
        this.hasMultimedia = hasMultimedia;
    }

    // Getters --------------------------------------------------------------------------
    public Library getLibrary() {
        return library;
    }

    public String getRoomNumber() {
        return roomNumber;
    }

    public RoomType getType() {
        return type;
    }

    public int getMaxPeople() {
        return maxPeople;
    }

    public boolean getAvailable() {
        return available;
    }

    public boolean getHasComputer() {
        return hasComputer;
    }

    public boolean getHasMultimedia() {
        return hasMultimedia;
    }

    // Setters --------------------------------------------------------------------------
    public void setAvailable(boolean newStatus) {
        this.available = newStatus;
    }
}