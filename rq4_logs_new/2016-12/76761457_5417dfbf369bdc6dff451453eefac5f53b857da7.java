package com.davis.datastructure.cipher;

import java.io.UnsupportedEncodingException;

public class SimpleCipher {

	public String encryption(String express,String sceretKey){
		try {
			byte[] expressBytes = express.getBytes("utf-8");
			byte[] sceretKeyBytes = sceretKey.getBytes("utf-8");
			byte[] ciphertextBytes = new byte[expressBytes.length];
			for(int i=0;i<expressBytes.length;i++){
				ciphertextBytes[i] = (byte)(expressBytes[i]^sceretKeyBytes[i%sceretKeyBytes.length]);
			}
			return new String(ciphertextBytes,"utf-8");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return "";
	}
	
	public String decrypt(String ciphertext,String sceretKey){
		try {
			byte[] expressBytes = ciphertext.getBytes("utf-8");
			byte[] sceretKeyBytes = sceretKey.getBytes("utf-8");
			byte[] ciphertextBytes = new byte[expressBytes.length];
			for(int i=0;i<expressBytes.length;i++){
				ciphertextBytes[i] = (byte)(expressBytes[i]^sceretKeyBytes[i%sceretKeyBytes.length]);
			}
			return new String(ciphertextBytes,"utf-8");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return "";
	}
	public static void main(String[] args) {
		SimpleCipher cipher = new SimpleCipher();
		String sceretKey = "test";
		String ciphertext = cipher.encryption("w1234dddd", sceretKey);
		System.out.println("ciphertext-->"+ciphertext);
		String express = cipher.decrypt(ciphertext, sceretKey);
		System.out.println("express-->"+express);
		
		int a = 16;
		System.out.println(a^12^12);
	}

}