/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/UnitTests/JUnit5TestClass.java to edit this template
 */
package edu.nwmissouri.zoo06group;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/**
 *
 * @author Bhuvan Chandra Sarakam S544899
 */
public class AdventureRidesTest {
    
    public AdventureRidesTest() {
    }
    
    @BeforeAll
    public static void setUpClass() {
    }
    
    @AfterAll
    public static void tearDownClass() {
    }
    
    @BeforeEach
    public void setUp() {
    }
    
    @AfterEach
    public void tearDown() {
    }

    /**
     * Test of resultOfGame method, of class AdventureRides.
     */
    @Test
    public void testResultOfGame() {
        System.out.println("resultOfGame");
        AdventureRides instance = new AdventureRides();
        String expResult = "You never Loose";
        String result = instance.resultOfGame();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        //fail("The test case is a prototype.");
    }

    /**
     * Test of onlineBooking method, of class AdventureRides.
     */
    @Test
    public void testOnlineBooking() {
        System.out.println("Welcome to adventure rides");
        AdventureRides instance = new AdventureRides();
        String expResult = "Your ticket is booked";
        String result = instance.onlineBooking();
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
       // fail("The test case is a prototype.");
    }

    /**
     * Test of run method, of class AdventureRides.
     */
   /* @Test
    public void testRun() {
        System.out.println("run");
        AdventureRides instance = new AdventureRides();
        instance.run();
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }*/
    
}