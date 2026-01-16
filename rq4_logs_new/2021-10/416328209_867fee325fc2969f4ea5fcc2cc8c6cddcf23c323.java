package com.pranay;

import static com.pranay.ThreadColors.ANSI_GREEN;

public class AnotherThread extends Thread{
    @Override
    public void run() {
        System.out.println(ANSI_GREEN+"Hello from another thread");
        try{
            Thread.sleep(3000);
        }catch (InterruptedException e){
            e.printStackTrace();
        }
        System.out.println("Three second have passed");
    }


}