package com.yearup;

public class ElaspsedTimer implements elapsed {

    private long startTime;
    @Override
    public void start() {
       startTime =  System.nanoTime();
    }

    @Override
    public void stop(String message) {
        long endTime = System.nanoTime();
        System.out.println(message+" took: "+ getMeasuredTime(startTime, endTime));
    }

    @Override
    public String getMeasuredTime(long startTime, long endTime) {
        double timeElapsed = endTime - startTime;
        //converting it to milliseconds
        timeElapsed/=1000;
        if(timeElapsed/1000 <= 1) return timeElapsed+" microseconds";
        timeElapsed/=1000;
        if(timeElapsed/1000 <= 1) return timeElapsed+" milliseconds";
        timeElapsed/=1000;
        if(timeElapsed/1000 <= 1) return timeElapsed+" seconds";

        return "na";
    }
}

interface elapsed{
    void start();
    void stop(String message);

    String getMeasuredTime(long startTime, long endTime);
}