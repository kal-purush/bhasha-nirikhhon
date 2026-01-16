/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ch.hearc.ig.indu.epicearc.business;

import ch.hearc.ig.odi.epicearc.business.ConiferSize;
import static org.junit.Assert.*;
import org.junit.Test;

/**
 *
 * @author johan.steiner
 */
public class TestConiferSize {
    
    @Test
    public void TestNewConiferSizeGetterSetter() {
        ConiferSize size = ConiferSize.MEDIUM;
        
        assertFalse(size == null);
        assertEquals(size.getCode().toUpperCase(), "MEDIUM");
        
        size.setCode("BIG");
        assertEquals(size.getCode().toUpperCase(), "BIG");
    }
    
}