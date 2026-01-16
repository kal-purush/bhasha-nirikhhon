package hw4;

public class HW4 {
    private volatile char currentLetter = 'A';

    public static void main(String[] args) {
        HW4 w = new HW4();

        Thread t1 = new Thread(()-> w.printABC('A','B'));
        Thread t2 = new Thread(()-> w.printABC('B','C'));
        Thread t3 = new Thread(()-> w.printABC('C','A'));

        t1.start();
        t2.start();
        t3.start();
    }


    public synchronized void printABC(char letter, char nextLetter) {
            for (int i = 0; i < 5; i++) {
                while (currentLetter !=letter) {
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }
                }
                System.out.println(letter);
                currentLetter = nextLetter;
                notifyAll();
            }
    }

}