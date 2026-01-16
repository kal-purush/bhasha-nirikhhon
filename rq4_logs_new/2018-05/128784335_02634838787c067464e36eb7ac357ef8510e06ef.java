import java.util.*;
/**
 * wait Function
 *
 * @author (Kathy Vuong)
 * @version (Something)
 */
public class wait
{
    /**
     * Method w allows for pausing
     *
     * @param time - amt of time to pause
     */
    public void w(int time)
    {
        try
        {
            Thread.sleep(time);
        }// Ends the try
        catch (InterruptedException e)
        {
            System.out.println("\f" + e.getMessage());
        }// Ends the catch
    }// Ends the w Method
}// Ends the wait Class