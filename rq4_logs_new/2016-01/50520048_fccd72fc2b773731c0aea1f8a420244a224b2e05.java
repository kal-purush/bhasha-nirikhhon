import TSim.*;
import java.awt.Point;
import java.util.concurrent.Semaphore;
public class Lab1 {
	
	public static Point s0 = new Point(13,3),s1 = new Point(6,6),s2 = new Point(11,7),s3 = new Point(14,7),s4  = new Point(19,9),s5 = new Point(18,9),s6 = new Point(12,9),s7 = new Point(7,9),s8 = new Point(1,10),s9 = new Point(1,11),s10 = new Point(6,11),s11 = new Point(12,11),s12 = new Point(9,5),s13 = new Point(10,8), s14 = new Point(15,8), s15 = new Point(13,10), s16 = new Point(6,10), s17 = new Point(5,13), s18 = new Point(12,14), s19 = new Point(14,5);
	public static Semaphore sem1 = new Semaphore(1,true), sem2 = new Semaphore(1,true), sem3 = new Semaphore(1,true), sem4 = new Semaphore(1,true), sem5 = new Semaphore(1,true), sem6 = new Semaphore(1,true);
//Sensors at (13,3), (7,7), (9,7), (16,7), (18,7), (16,9), (15,10), (5,10), (3,9), (2,11), (3,13), (12,3), (8,6)
// (14,5), (9,8), (17,8), (14,9), (5,9), (4,11), (12,11)
//Switches at (17,7), (15,9), (4,9), (3,11)
//If train direction = 1. Lower speed at sensor (13,3)
//One main road, meaning no turns are needed. If main road is occupied (check (tryAcquire) semaphore), take 
//alternate route if possible. Otherwise, wait if main road is only possible way to go (acquire(semaphore)).
	

	public Lab1(Integer speed1, Integer speed2) {
		TSimInterface tsi = TSimInterface.getInstance();
		
		try {
			Train train1 = new Train(speed1, 1);
			Train train2 = new Train(speed2, 2);
			Thread t1 = new Thread(train1);
			Thread t2 = new Thread(train2);

			
			t1.start();
			t2.start();
			

		}catch (CommandException e) {
			e.printStackTrace();    // or only e.getMessage() for the error
			System.exit(1);
		}
	}
}