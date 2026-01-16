import runnable.DataReceiver;

public class Server {

    public static void main(String[] args) {
        DataReceiver dataReceiver = new DataReceiver();
        Thread thread1 = new Thread(dataReceiver);
        thread1.start();


    }
}