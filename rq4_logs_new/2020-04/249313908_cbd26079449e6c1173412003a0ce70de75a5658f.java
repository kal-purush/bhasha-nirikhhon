package edu.neu.coe.ranking;

import org.junit.Test;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.*;

public class PredictionSpec {

    @Test
    public void testPrediction() throws Exception {
        Prediction t = new Prediction();
        ReadCsv readcsv = new ReadCsv();
        Map<String, Map<Integer, Integer>> hmap = readcsv.hmap;
        Map<String, Map<Integer,Integer>> amap = readcsv.amap;
        int res = t.result("Liverpool", "Chelsea", hmap, amap);

        List<Integer> resultList = Arrays.asList(0, 1, 3);
        // Check List has this value
        assertTrue(resultList.contains(res));
    }

    @Test
    public void testMultiplePrediction() throws Exception {
        Prediction t = new Prediction();
        ReadCsv readcsv = new ReadCsv();
        Map<String, Map<Integer, Integer>> hmap = readcsv.hmap;
        Map<String, Map<Integer,Integer>> amap = readcsv.amap;

        int win = 0, lose = 0, draw = 0;
        for (int i = 0; i < 200; i++) {
            int res = t.result("Liverpool", "Norwich", hmap, amap);
            if (res == 3) {
                win++;
            } else if (res == 1) {
                draw++;
            } else {
                lose++;
            }
        }
//        System.out.println("win: " + win);
//        System.out.println("draw: " + draw);
//        System.out.println("lose: " + lose);

        //2. Check List has this value
        assertTrue(win > 100 && win <= 200);
        assertTrue(draw >= 0 && draw <= 50);
        assertTrue(lose >= 0 && lose <= 20);
    }
}