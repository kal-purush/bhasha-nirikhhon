package themes.oopbuilding;

public class Apartment {

    private final int numberApart;
    private final Room[] rooms;

    public Apartment(int numberApart, Room[] rooms) {
        this.numberApart = numberApart;
        this.rooms = rooms;
    }

    public Room[] getRooms() {
        return rooms;
    }

    public void printApart() {
        System.out.println("Номер квартиры " + numberApart + "\nКоличество комнат: " + rooms.length);
    }
}