package com.prog.lessons.customlist;

import org.junit.Before;
import org.junit.Test;
import java.util.Calendar;


import java.util.ArrayList;

import static org.junit.Assert.*;

public class ArraylistTest {
    ArrayList<Integer> newList;
    long startTime = System.currentTimeMillis();
    @Before
    public void setUp() throws Exception {

        newList = new ArrayList<>();
        for(int i =0; i<1000000; i++){
            newList.add(i);
        }

    }


    @Test
    public void add() {

    assertEquals(1000000, newList.size());
                long totalTime = System.currentTimeMillis() - startTime;
        Calendar cal = Calendar.getInstance();
        cal.setTimeInMillis( totalTime );
        int minutes = cal.get(Calendar.MINUTE);
        int seconds = cal.get(Calendar.SECOND);
        int ms = cal.get(Calendar.MILLISECOND);
        System.out.println (minutes + ":" + seconds + ":" + ms);

    }
}