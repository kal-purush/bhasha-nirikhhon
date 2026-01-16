package ElevatorProject;

import java.util.ArrayDeque;

/** 
 * Group 1 - Nicholas Persing, Christopher Millsap, Julio Villazon
 *Elevator Project CSE 2010 - Fall 2015 - Section 1
 * 
 * Created by: Nicholas Persing @ 8/31/2015 11:52
 *      -basic structure, setters and getters
 * Edited by: 
 */
public class FloorQueue {
    ArrayDeque<Person> upQueue, downQueue;
    public FloorQueue(){
        upQueue = new ArrayDeque<Person>() {};
        downQueue = new ArrayDeque<Person>() {};
    }
    
    public int EmptyInto(Elevator e, Direction d){
        int count = 0;
        switch(d){
            case UP:
                while(!upQueue.isEmpty() && !e.isFull()){
                    //TODO: add to Elevator
                    upQueue.removeFirst();
                }
            case DOWN: 
                while(!downQueue.isEmpty() && !e.isFull()){
                    //TODO: add to Elevator
                    downQueue.removeFirst();
                }
        }
        return count;
    }
            
    
}