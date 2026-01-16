
/**
 * Write a description of class LockedRoom here.
 * 
 * @author Gavin Kyte
 * @version 2017.3.26
 */
public class LockedRoom extends Room
{
    private boolean isLocked;
    private Item keyToUnlock = null;
    private String roomReqs;

    /**
     * Constructor for objects of class LockedRoom
     */
    public LockedRoom(String title, String description, boolean isLocked){
        super(title, description);
        this.isLocked = isLocked;
        roomReqs = "You need the " + keyToUnlock + "to unlock this room.";
    }
    
    public boolean meetsRequirements(){
        return !isLocked;
    }

        public String getRequirements(){
        return roomReqs;
    }

    /**
     * Unlocks the room if the player is holding the key object
     * that has been linked to this room.
     * @return String with lock status of room
     */
    public String unlock(){
        isLocked = false;
        return "Room unlocked";
    }

	/**
     * Locks the room if the player is holding the key object
     * that has been linked to this room.
     * @return String with lock status of room
     */
	public String lock(){
		isLocked = true;
		return "Room locked.";
    }

	public void setKey(Item key){
        this.keyToUnlock = key;
    }
    
    public String getKey(){
        return keyToUnlock.getName();
    }        
        
    public void setRequirements(String newReqs){
        this.roomReqs = newReqs;
    }
}