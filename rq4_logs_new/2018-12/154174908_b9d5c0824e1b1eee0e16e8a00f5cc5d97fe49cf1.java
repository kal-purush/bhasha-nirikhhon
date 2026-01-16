package com.example.cs121.flarevisualizer;

import org.junit.Test;

import java.sql.Timestamp;
import java.util.List;

import static org.junit.Assert.*;

public class FlareClassUnitTest {

    FlareClass flare;

    @Test
    public void updateFlare() {
        // before flare is updated
        flare = new FlareClass();
        List<String> pain_Nums = flare.getPain_Nums();
        assertEquals(pain_Nums.size(), 0);
        List<String> times = flare.getTimes();
        assertEquals(times.size(), 0);
        List<String> actTriggers = flare.getActTriggers();
        assertEquals(actTriggers.size(), 0);
        List<String> dietTriggers = flare.getDietTriggers();
        assertEquals(dietTriggers.size(), 0);
        List<String> miscTriggers = flare.getMiscTriggers();
        assertEquals(miscTriggers.size(), 0);
        String dbId = flare.getDbId();
        assertEquals(dbId, "");

        // update
        int time = (int) (System.currentTimeMillis());
        Timestamp ts = new Timestamp(time);
        actTriggers.add("act");
        dietTriggers.add("diet");
        miscTriggers.add("misc");
        flare.updateFlare("10", ts, "a", actTriggers, dietTriggers, miscTriggers);

        // after flare is updated
        pain_Nums = flare.getPain_Nums();
        assertEquals(pain_Nums.size(), 1);
        times = flare.getTimes();
        assertEquals(times.size(), 1);
        actTriggers = flare.getActTriggers();
        assertEquals(actTriggers.size(), 1);
        dietTriggers = flare.getDietTriggers();
        assertEquals(dietTriggers.size(), 1);
        miscTriggers = flare.getMiscTriggers();
        assertEquals(miscTriggers.size(), 1);
        dbId = flare.getDbId();
        assertEquals(dbId, "a");
    }

    @Test
    public void addAndGetPain_Nums() {
        flare = new FlareClass();
        List<String> pain_Nums = flare.getPain_Nums();
        assertEquals(pain_Nums.size(), 0);

        flare.addPain_Nums("10");
        pain_Nums = flare.getPain_Nums();
        assertEquals(pain_Nums.size(), 1);
    }

    @Test
    public void addAndGetTime() {
        flare = new FlareClass();
        List<String> times = flare.getTimes();
        assertEquals(times.size(), 0);

        int time = (int) (System.currentTimeMillis());
        Timestamp ts = new Timestamp(time);

        boolean output = flare.addTime(ts);
        assertTrue(output);

        // new time is chronologically after the previous time
        time += 100;
        ts = new Timestamp(time);
        output = flare.addTime(ts);
        assertTrue(output);

        // new time is not chronologically after the most recent time
        time -= 101;
        ts = new Timestamp(time);
        output = flare.addTime(ts);
        assertFalse(output);

        times = flare.getTimes();
        assertEquals(times.size(), 2);
    }

    @Test
    public void setAndGetDbId() {
        flare = new FlareClass();
        String dbId = flare.getDbId();
        assertEquals(dbId, "");

        flare.setDbId("a");
        dbId = flare.getDbId();
        assertEquals(dbId, "a");
    }
}