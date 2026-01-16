/* SimClock
 * A simulated clock for tracking virtual time down to the nanosecond
 */

public class SimClock{
    private double time; //The current "time"
    private double deltaTime; //The default time increment
    
    /*
    * @param td: the default time increment when calling SimClock.update()
    */
    public SimClock(double dt){
        deltaTime = dt;
    }
    
    //Increments the clock based on the deltaTime value
    public void update(){
        time += deltaTime;
    }
    
    //Setting the first piece of time data to create the proper start time
    public void setTime(double firstTime){
        time = firstTime;
    }
    
    //returns the current "time"
    public double getTime(){
        return time;
    }
    

}