/**
 * Basic thread example.
 * @author ashish
 */
public class ThreadSimple {
    public static void print()
    {
        for (int i = 0; i < 10; i++) {
            System.out.println("Hello");
        }

    }
    public static void main(String[] args) {

        Thread t1=new Thread(ThreadSimple::print);
        Thread t2=new Thread(ThreadSimple::print);
        //start both the threads
        t1.start();
        t2.start();
    }
}