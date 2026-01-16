
/**
 * Write a description of class Player here.
 * 
 * @author (your name) 
 * @version (a version number or a date)
 */
public class Player
{
    // instance variables - replace the example below with your own
    private String name;
    private String currentRoom;

    /**
     * Constructor for objects of class Player
     */
    public Player(String name, String room){
        this.name = name;
        currentRoom = room;
    }

    /**
     * Gets current room name
     * @return     String currentRoom
     */
    public String getRoom(){
        return currentRoom;
    }
    
    /**
     * Sets current room
     * @param String room title
     */
    public void setRoom(String roomName){
        this.currentRoom = roomName;
    }
}