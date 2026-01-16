/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ElevatorProject;

import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author nicholas
 */
public class ElevatorControl {
    private FloorQueue[] floorArray;
    private Elevator elevator;
    private static int tick;
    private final int floorCount;
    
    public ElevatorControl(int cap,int floorCount){
        try{
            elevator = new Elevator(cap, floorCount);
            floorArray = new FloorQueue[floorCount];
            
            for(int i = 0; i<cap; i++)
                floorArray[i] = new FloorQueue(i);
            
        }catch(InvalidCapacityException | InvalidLocationException e){
            System.out.println("Could not create elevator! Please see "
                    + "system out put for more details about the error");
        }
        this.floorCount = floorCount;
    }
    
    public int getTick(){
        return tick;
    }
    
    private void getButtonsPressed(){
        for(int i = 0; i)
    }
    
    public void RunSimple(int passengerRate, int duration, int maxPerFloor){
        Random rand = new Random();
        int temp;
        for(;tick<duration;tick++){
            for(int i = 0; i< floorCount; i++) //maybe stick this in it's own method method
                for(int j=0; j<maxPerFloor; j++){
                    if(rand.nextInt(100) < passengerRate){
                        temp = rand.nextInt(floorCount);
                        temp++;
                        try {
                            floorArray[i].addPerson(new Person(temp, tick), temp);
                            //need to make a check in the future to be sure a passergner does not
                            //want to get off on their current floor
                        } catch (InvalidLocationException ex) {
                            System.out.println("Invalid Location Exception caught");
                        }
                    }    
                }//This ends populating the floors with new passengers
                      
              
            
        }//This is where the tick loop ends
        
    }
    
    
}