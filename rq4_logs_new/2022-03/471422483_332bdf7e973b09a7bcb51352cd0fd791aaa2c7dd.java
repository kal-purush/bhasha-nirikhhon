package automation.test;

import java.awt.AWTException;
import java.awt.Robot;
import java.awt.event.KeyEvent;
import java.io.IOException;

/**
 *
 * @author nathee
 */
public class KeyBoardControlExample {

    public static void main(String args[]) throws IOException, AWTException, InterruptedException {
        String command = "notepad.exe";
        Runtime run = Runtime.getRuntime();
        run.exec(command);
        try {
            //delay 4ms  
            Thread.sleep(4000);
        } catch (InterruptedException e) {
            // TODO Auto-generated catch block   
            e.printStackTrace();
        }
        //creating a constructor of the Robot class  
        Robot robot = new Robot();
        //pressing key by invoking the keyPress() method  
        robot.keyPress(KeyEvent.VK_W);
        //delay of 5 miliseconds after each key press  
        Thread.sleep(100);
        robot.keyPress(KeyEvent.VK_E);
        Thread.sleep(100);
        robot.keyPress(KeyEvent.VK_L);
        Thread.sleep(100);
        robot.keyPress(KeyEvent.VK_C);
        Thread.sleep(100);
        robot.keyPress(KeyEvent.VK_O);
        Thread.sleep(100);
        robot.keyPress(KeyEvent.VK_M);
        Thread.sleep(100);
        robot.keyPress(KeyEvent.VK_E);
        Thread.sleep(100);
        robot.keyPress(KeyEvent.VK_SPACE);
        Thread.sleep(100);
        robot.keyPress(KeyEvent.VK_T);
        Thread.sleep(100);
        robot.keyPress(KeyEvent.VK_O);
        Thread.sleep(100);
        robot.keyPress(KeyEvent.VK_SPACE);
        Thread.sleep(100);
        robot.keyPress(KeyEvent.VK_J);
        Thread.sleep(100);
        robot.keyPress(KeyEvent.VK_A);
        Thread.sleep(100);
        robot.keyPress(KeyEvent.VK_V);
        Thread.sleep(100);
        robot.keyPress(KeyEvent.VK_A);
        Thread.sleep(100);
        robot.keyPress(KeyEvent.VK_T);
        Thread.sleep(100);
        robot.keyPress(KeyEvent.VK_P);
        Thread.sleep(100);
        robot.keyPress(KeyEvent.VK_O);
        Thread.sleep(100);
        robot.keyPress(KeyEvent.VK_I);
        Thread.sleep(100);
        robot.keyPress(KeyEvent.VK_N);
        Thread.sleep(100);
        robot.keyPress(KeyEvent.VK_T);
        Thread.sleep(100);
    }
}