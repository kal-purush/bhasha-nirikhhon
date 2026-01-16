package cscie55.hw2;

/**
 * @Author ssurabhi on 10/3/15.
 * @version 1.0
 */

public class Building {

    private Elevator elevator;
    private Floor[] floors;
    public static final int FLOORS = 7;

    /*The Building constructor creates an Elevator, and one floor for each floor number.*/
    public Building(){
        this.elevator = new Elevator(this);
            floors = new Floor[FLOORS+1];
            for (int i=0; i<FLOORS+1; i++) {
                floors[i] = new Floor(this,i);
            }
        }

    /*Returns the building's Elevator
     @param
    */
    public Elevator elevator() {
        return elevator;
    }

   /*
   Returns the Floor object for the given floor number
   @param
    */
   public Floor floor(int floorNumber){
       return this.floors[floorNumber];
   }

    public  Floor[] getFloors() {
        return floors;
    }

    public  void setFloors(Floor[] floors) {
        this.floors = floors;
    }
}