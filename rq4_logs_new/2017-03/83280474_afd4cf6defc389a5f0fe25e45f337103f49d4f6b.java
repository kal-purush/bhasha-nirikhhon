package com.cybage.core;

import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.text.SimpleDateFormat;
import java.util.Scanner;

import org.apache.log4j.Logger;

public class CryptographySHA1 {

	static Logger logger = Logger.getLogger(CryptographySHA1.class);
	
	/**
	 * @param args
	 */
	public static void main(String[] args) {

		if(logger.isDebugEnabled()){
			logger.debug("Entered into JVM call");
		}
		
		Scanner scanner = new Scanner(System.in);
		System.out.println("Please enter Username:");
		String username = scanner.next();
		
		convertStringToSHA1(username);

	}
	
	private static void convertStringToSHA1(String username){
	        String sTimeStamp  = new SimpleDateFormat("MM/dd/yyyy HH:MM:SS").format(new java.util.Date());
	        String sStringToHash = String.format("%1$s\n%2$s", username, sTimeStamp);

	        MessageDigest cript = null;
			try {
				cript = MessageDigest.getInstance("SHA-1");
			} catch (NoSuchAlgorithmException e) {
				e.printStackTrace();
			}
	        cript.reset();

	        try {
				cript.update(sStringToHash.getBytes("utf-8"));
			} catch (UnsupportedEncodingException e) {
				e.printStackTrace();
			}
	        sStringToHash = new BigInteger(1, cript.digest()).toString(16);
	        
	        System.out.println("Sha Value:\n"+sStringToHash);
	}

}