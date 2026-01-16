import greenfoot.*;  // (World, Actor, GreenfootImage, Greenfoot and MouseInfo)
import java.util.Random;
/**
 * Write a description of class APPLE here.
 * 
 * @author (your name) 
 * @version (a version number or a date)
 */
public class APPLE extends Actor
{
     Random random = new Random();
     int rand = random.nextInt(360);
    /**
     * Act - do whatever the APPLE wants to do. This method is called whenever
     * the 'Act' or 'Run' button gets pressed in the environment.
     */
    public void act()
    {
        setRotation(rand);
        move(5);
    }
}