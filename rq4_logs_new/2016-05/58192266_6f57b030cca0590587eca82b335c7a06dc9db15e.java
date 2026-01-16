package com.traffic;
 
public class Initial {
 
    /**
     * @param args
     */
    public static void main(String[] args) {
        // Create objects
        Traffic car = new Car();
        Traffic airplane = new Airplane();
        Traffic boat = new Boat();
        //set passenger capacity
        car.setPC(8);
        airplane.setPC(400);
        boat.setPC(200);
        //call function whoAmI()
        car.whoAmI(car);
        airplane.whoAmI(airplane);
        boat.whoAmI(boat);
    }
 
}