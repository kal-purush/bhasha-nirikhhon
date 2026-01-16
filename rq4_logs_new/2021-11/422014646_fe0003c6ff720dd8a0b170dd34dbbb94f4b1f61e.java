/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/Classes/Class.java to edit this template
 */
package edu.nwmissouri.zoo06group;

import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

/**
 *
 * @author Revanth Pagilla
 */
public class LigerGroupTest {
     public LigerGroupTest() {
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
     * Test of create method, of class LigerGroup
     */
    @Test
    public void testGroupCreate() {
        int expectedSize = 3;
        int actualSize = LigerGroup.create();
        assertEquals(expectedSize, actualSize);
    }

    /**
     * Test of run method, of class LigerGroup.
     */
    @Test
    public void testGroupRun() {
        LigerGroup.create();
        LigerGroup.run();
        assertTrue(true);
    }

}
