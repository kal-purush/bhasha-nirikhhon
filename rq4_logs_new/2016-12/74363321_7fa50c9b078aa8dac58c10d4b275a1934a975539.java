/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package ch.hearc.ig.indu.epicearc.business;

import ch.hearc.ig.odi.epicearc.business.AbstractDate;
import ch.hearc.ig.odi.epicearc.business.ConiferType;
import ch.hearc.ig.odi.epicearc.business.DeliveryDate;
import ch.hearc.ig.odi.epicearc.business.PickupDate;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import static org.junit.Assert.*;
import org.junit.Test;

/**
 *
 * @author johan.steiner
 */
public class TestDate {
    
    @Test
    public void TestNewDeliveryDateGetterSetter() throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("dd.MM.yyyy");
        
        DeliveryDate dateDeliv = new DeliveryDate();
        assertFalse(dateDeliv == null);
        
        dateDeliv.setId(Long.valueOf(1));
        dateDeliv.setDate(sdf.parse("13.09.2015"));
        dateDeliv.setConiferType(ConiferType.SPRUCE);
        
        assertEquals(dateDeliv.getId(), Long.valueOf(1));
        assertEquals(dateDeliv.getDate(), sdf.parse("13.09.2015"));
        assertEquals(dateDeliv.getFormatedDate(), "13.09.2015");
        assertEquals(dateDeliv.getConiferType().getCode().toUpperCase(), "SPRUCE");
    }
    
    @Test
    public void TestNewPickupDateGetterSetter() throws ParseException {
        SimpleDateFormat sdf = new SimpleDateFormat("dd.MM.yyyy");

        PickupDate datePickup = new PickupDate(Long.valueOf(1), sdf.parse("15.09.2015"), ConiferType.SPRUCE);
        
        assertFalse(datePickup == null);
        
        datePickup.setId(Long.valueOf(1));
        datePickup.setDate(sdf.parse("15.09.2015"));
        datePickup.setConiferType(ConiferType.SPRUCE);
        
        assertEquals(datePickup.getId(), Long.valueOf(1));
        assertEquals(datePickup.getDate(), sdf.parse("15.09.2015"));
        assertEquals(datePickup.getFormatedDate(), "15.09.2015");
        assertEquals(datePickup.getConiferType().getCode().toUpperCase(), "SPRUCE");
    }
    
}