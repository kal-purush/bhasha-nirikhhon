package org.usfirst.frc.team5483.robot.utils;

import java.io.File;
import java.io.FileWriter;
import java.io.BufferedWriter;
import java.io.IOException;

public class CommandLogger {
	private File cmdLogFile;
	private FileWriter toWrite;
	private BufferedWriter writer;
	
	public CommandLogger() {
		/*Flashmedia/sda1*/
		cmdLogFile = new File("media/sda1/cmdlog.txt");
		try {
			if (!cmdLogFile.exists()) {
				cmdLogFile.createNewFile();
			}
			toWrite = new FileWriter(cmdLogFile);
			writer = new BufferedWriter(toWrite);
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	public CommandLogger(File name) {
		cmdLogFile = name;
		
	}
	public void logCommand(String command) {
		try {
			writer.write(command);
			writer.flush();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}
	
	public void closeCmdLogger() {
		try {
			writer.close();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

}