/*  +__^_________,_________,_____,________^-.-------------------,
 *  | |||||||||   `--------'     |          |                   O
 *  `+-------------USMC----------^----------|___________________|
 *    `\_,---------,---------,--------------'
 *      / X MK X /'|       /'
 *     / X MK X /  `\    /'
 *    / X MK X /`-------'
 *   / X MK X /
 *  / X MK X /
 * (________(                @author m.c.kunkel
 *  `------'
*/
package org.jlab.dc_calibration;

public class App {

	public static void main(String[] arg) {
		// new aSimpleJavaConsole(); // create console with not reference
		// kp:
		DC_CalibrationNew cnt = new DC_CalibrationNew(); // create console with not reference
		// System.out.println("Main thread run is starting now ...");
		// int kpLoopBreaker = 0;
		// try {
		// while (cnt.reader.isAlive() || cnt.reader2.isAlive()) {
		// System.out.println("Main thread will be alive till the child thread is live");
		// Thread.sleep(1500);
		//
		// for (int i = 0; i < 4; i++) {
		// System.out.println("Printing the count " + i);
		// System.out.println("Printing the count with my own System.out.println (KpLib1) " + i); // method defined in my own library
		// Thread.sleep(1000);
		// }
		// kpLoopBreaker++;
		// System.out.println("========kpLoopBreaker = " + kpLoopBreaker + "=============");
		// // if(kpLoopBreaker>3) break;
		// if (kpLoopBreaker > 300)
		// break;
		// }
		// } catch (InterruptedException e) {
		// System.out.println("Main thread interrupted");
		// }
		// System.out.println("Main thread run is over");
		// System.out.println("========Now this program will sleep for 5 secs and then exit=============");
		//
		// // kp:
		// // http://www.java67.com/2015/06/how-to-pause-thread-in-java-using-sleep.html
		// try {
		// // Let's wait to see game thread stopped
		// TimeUnit.MILLISECONDS.sleep(5000); // kp: Sleep for 5 seconds
		// exit(0);
		// } catch (InterruptedException ex) {
		// Logger.getLogger(DC_CalibrationNew.class.getName()).log(Level.SEVERE, null, ex);
		// }

	}

}