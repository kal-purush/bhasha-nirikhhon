package com.chris.oscjp.chapter5;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.Console;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStreamReader;

public class FileExercise {

	private static String fileName;
	private char c;
	
	public static void main(String[] args) {
		FileExercise fe = new FileExercise();
		fe.promptForFileName();
		fileName = fe.captureFilename();
		fe.promptForData();
		fe.readAndWrite(fileName);
		fe.promptForFileToOpen();
	}
	public void promptForFileName(){
		System.out.println("Please Enter a Filename which will be used to save the text you enter :");
	}
	public void promptForFileToOpen(){
		
		FileReader fr = null;
		
		System.out.println("Please Enter a Filename which will be used to read :");
		
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

	      try {
	         fileName = br.readLine();
	      } catch (IOException ioe) {
	         System.out.println("IO error trying to read filename!");
	         ioe.printStackTrace();
	      }
	      File file = new File(fileName);
	      System.out.println("Opening File " + fileName);
	      
	      try {
			fr = new FileReader(file);
			System.out.println("Creating File Reader " + fr);
		} catch (FileNotFoundException fnf) {
			// TODO Auto-generated catch block
			fnf.printStackTrace();
		}
	      
	    BufferedReader fread = new BufferedReader(fr);
	      try {
			System.out.println("File Contains " + fread.readLine());
			fread.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public void promptForData(){
		System.out.println("Please Enter Text to be written to your file :");
	}
	public String captureFilename(){
		String fileName = null;
		
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));

	      try {
	         fileName = br.readLine();
	      } catch (IOException ioe) {
	         System.out.println("IO error trying to read filename!");
	         ioe.printStackTrace();
	      }
	      return fileName;
	}
	public void readAndWrite(String fileName){
		
		FileWriter fw = null;
		char c = ' ';
		String output = null;
		
		// Create a file to store the text entered
		File file = new File(fileName);
		try{ 
		file.createNewFile();
		System.out.println("Creating file " + fileName);
		} catch (IOException ioe){
			System.out.println("IO error trying to create file!");
	         ioe.printStackTrace();
		}
			
		try {
			fw = new FileWriter(file);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		 
		// Read & Write the input whilst input char != ESC
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
				
		while(true){
			try {
				c = (char) br.read();
			} catch (IOException ioe) {
				// TODO Auto-generated catch block
				ioe.printStackTrace();
			}
			if(c == (int) 32){
				System.out.println("DEL entered !!! ");
				try {
					fw.flush();
					fw.close();
				} catch (IOException ioe) {
					// TODO Auto-generated catch block
					ioe.printStackTrace();
				}
				break;
			}
			else 
				try {
				//	output = c;
					output =  (c + br.readLine());
					System.out.println("String entered was " + output);
					fw.write(output);

				} catch (IOException ioe) {
					// TODO Auto-generated catch block
					ioe.printStackTrace();
				}
		}
	}	
		
}
		

