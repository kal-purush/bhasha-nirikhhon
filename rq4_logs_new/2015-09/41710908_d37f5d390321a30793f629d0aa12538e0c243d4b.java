/**
 * Group 2 - Nicholas Persing, Christopher Millsap, Julio Villazon
 *Elevator Project CSE 2010 - Fall 2015 - Section 1
 * 
 */
package ElevatorProject;

class EmptyQueueException extends Exception {

    public EmptyQueueException() {
        System.out.println("Can't Remove from empty array!");
    }
    
    public EmptyQueueException(String s){
        System.out.println("Can't Remove from empty array!");
        System.out.println(s);
    }
    
}