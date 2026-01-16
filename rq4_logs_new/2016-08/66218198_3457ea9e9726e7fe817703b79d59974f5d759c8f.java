package com.company;

import java.io.File;

public class CountAlgorithm {

    CsvClass csvClass = new CsvClass();
    String fileName = "AddCollections.csv";

    File file = new File(fileName);

    public int maxIteration = 1024000;
    public int startElement = 600;
    public long iterations10K = 256;
    public long iterations100K = 2560;
    public long iterations1000K = 25600;
    public long startTime = 0;
    public long finishTime10K = 0;
    public long finishTime100K = 0;
    public long finishTime1000K = 0;


    public void timeCount(int i) {
        startTime = ifStartElement(startTime, i);
        finishTime10K = ifStartElement10K(startTime, finishTime10K, i);
        finishTime100K = ifStartElement100K(startTime, finishTime100K, i);
        finishTime1000K = ifStartElement1000K(startTime, finishTime1000K, i);
    }

    public long ifStartElement(long startTime, int i) {
        if (i == startElement) {
            startTime = System.nanoTime();
        }
        return startTime;
    }

    public long ifStartElement10K(long startTime, long finishTime10K, int i) {
        if (i == startElement + iterations10K) {
            long tempFinishTime10K = System.nanoTime() - startTime;
            if (i != 0) {
                finishTime10K = (finishTime10K + tempFinishTime10K) / 2;
            } else {
                finishTime10K = tempFinishTime10K;
            }
        }
        return finishTime10K;
    }

    public long ifStartElement100K(long startTime, long finishTime100K, int i) {
        if (i == startElement + iterations100K) {
            long tempFinishTime100K = System.nanoTime() - startTime;
            if (i != 0) {
                finishTime100K = (finishTime100K + tempFinishTime100K) / 2;
            } else {
                finishTime100K = tempFinishTime100K;
            }
        }
        return finishTime100K;
    }

    public long ifStartElement1000K(long startTime, long finishTime1000K, int i) {
        if (i == startElement + iterations1000K) {
            long tempFinishTime1000K = System.nanoTime() - startTime;
            if (i != 0) {
                finishTime1000K = (finishTime1000K + tempFinishTime1000K) / 2;
            } else {
                finishTime1000K = tempFinishTime1000K;
            }
        }
        return finishTime1000K;
    }

    public void outPtintTime(long finishTime10K, long finishTime100K, long finishTime1000K) {
        System.out.println("The mean value time 10K = " + finishTime10K);
        System.out.println("The mean value time 100K = " + finishTime100K);
        System.out.println("The mean value time 1000K = " + finishTime1000K);
    }

    public void writrTime() {
        outPtintTime(finishTime10K, finishTime100K, finishTime1000K);
        csvClass.write(file, String.valueOf(finishTime10K) + ';' + String.valueOf(finishTime100K) + ';'
                + String.valueOf(finishTime1000K) + ';' + "\n");
    }
}
