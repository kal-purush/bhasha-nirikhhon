package u_2024_05_22.homework;

public class CountingThread extends Thread {

    private int start;
    private int finish;

    public void setBounds(int a, int b) {
        start = a;
        finish = b;
    }

    @Override
    public void run() {
        for (int i = start; i<=finish; i++) {
            if (i%21 == 0 && Main_Two_Threads.threeCheck(i) == true ) {
                Main_Two_Threads.increment();
            }
        }
    }
}