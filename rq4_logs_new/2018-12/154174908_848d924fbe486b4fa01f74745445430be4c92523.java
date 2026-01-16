package com.example.cs121.flarevisualizer;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.*;

public class EditActivityUnitTest {

    EditActivity editAct;

    @Before
    public void setUp() throws Exception {
        editAct = new EditActivity();
    }

    @Test
    public void getStartAndEnd() {
        List<String> input = new ArrayList<>();
        List<String> output = editAct.getStartAndEnd(input);
        assertTrue(output.isEmpty());
        input.add("a");
        input.add("b");
        input.add("c");
        input.add("d");
        output = editAct.getStartAndEnd(input);
        assertFalse(output.isEmpty());
        assertEquals(output.size(), 2);
        assertEquals(output.get(0), "a");
        assertEquals(output.get(1), "d");
    }

    @Test
    public void getPainAvg() {
        List<String> input = new ArrayList<>();
        int output = editAct.getPainAvg(input);
        assertEquals(output, 0);
        input.add("1");
        output = editAct.getPainAvg(input);
        assertEquals(output, 1);
        input.add("2");
        input.add("3");
        input.add("4");
        input.add("5");
        input.add("6");
        input.add("7");
        input.add("8");
        input.add("9");
        input.add("10");
        output = editAct.getPainAvg(input);
        assertEquals(output, 55/10);
    }
}