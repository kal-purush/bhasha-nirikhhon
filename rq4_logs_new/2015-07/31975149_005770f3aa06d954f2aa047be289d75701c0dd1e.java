package javaEmail;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;

public class SendEmail {  
	/**
	 * Api for sending email
	 * @param from sender
	 * @param to   receiver
	 * @param message 
	 * @param noc  number of channels
	 * @param icq  whether to plot ic vs q
	 * @param gmap whether to plot google map
	 * @return true if everything works
	 */
	public static boolean send(String from, String to, String message, int noc, boolean icq, boolean gmap) {
		if (from == null || from.length() == 0 || to == null || to.length() == 0) return false;
		if (message == null || message.length() == 0) return false;
		if (noc < 0) return false;
        String s = null;
        String ICQ = "NICvsQ";
        String googleMap = "NgMap";
        try {
        	if (icq) ICQ = "YICvsQ";
        	if (gmap) googleMap = "YgMap";
            // using the Runtime exec method:
            Process p = Runtime.getRuntime().exec("python C:\\Users\\Administrator\\Desktop\\motoDemo\\python\\send.py " + from + " " + to + " " + Integer.toString(noc) + " " + ICQ + " " + googleMap + " " + message);
             
            BufferedReader stdInput = new BufferedReader(new
                 InputStreamReader(p.getInputStream()));
 
            BufferedReader stdError = new BufferedReader(new
                 InputStreamReader(p.getErrorStream()));
 
            // read the output from the command
            // System.out.println("Here is the standard output of the command:\n");
            while ((s = stdInput.readLine()) != null) {
                System.out.println(s);
            }
             
            // read any errors from the attempted command
            // System.out.println("Here is the standard error of the command (if any):\n");
            while ((s = stdError.readLine()) != null) {
                System.out.println(s);
            }
            return true;
        }
        catch (IOException e) {
            System.out.println("exception happened!");
            e.printStackTrace();
            return false;
        }
	}
}