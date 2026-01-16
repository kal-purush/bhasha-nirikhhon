/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;

/**
 *
 * @author soufiane
 */
public class AeroportTest {
    
    public AeroportTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    /**
     * Test of Vol method, of class Aeroport.
     */
    @Test
    public void testVol() {
        System.out.println("Vol");
        Aeroport instance = new Aeroport();
        instance.Vol();
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of ouvrirReservation method, of class Aeroport.
     */
    @Test
    public void testOuvrirReservation() {
        System.out.println("ouvrirReservation");
        Aeroport instance = new Aeroport();
        instance.ouvrirReservation();
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of fermerReservation method, of class Aeroport.
     */
    @Test
    public void testFermerReservation() {
        System.out.println("fermerReservation");
        Aeroport instance = new Aeroport();
        instance.fermerReservation();
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }
    
}