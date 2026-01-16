
package customer;

import java.util.Random;

/**
 *
 * @author Ben Reining
 * @version 11/6
 * 
 */



public class Customer {
    
    private int items = 0;
    private int timeRemaining = 0;
    Random gen = new Random();
    private int maxItems = 20;
    
    /**
     * Constructor
     * @param items
     * @param timeRemaining 
     */
    public Customer (int items, int timeRemaining){
        this.items = items;
        this.timeRemaining = timeRemaining;
    }
    /**
     * Getter
     * @return items
     */
    public int getItems(){
        return items;
    }
    /**
     * Getter
     * @return timeRemaining
     */
    public int getTime(){
        return timeRemaining;
    }
    /**
     * Method used to decrement the time after each processed item.
     * @param timeRemaining
     * @return timeRemaining
     */
    public int decrementTime(int timeRemaining){
        items = gen.nextInt(maxItems) + 1;
        timeRemaining = 0;
        
        
        return timeRemaining;
    }
    
    
    
   
    
    
}