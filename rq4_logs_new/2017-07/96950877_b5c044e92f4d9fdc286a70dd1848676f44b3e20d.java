package com.cicdproject.BankPortal.utities;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

public class HTMLUtils {
	public static String renderErrorPage(String errorMessage) {
		StringBuilder sb = new StringBuilder();
		
		sb.append("<p>");
		sb.append(errorMessage);
		sb.append("</p>");
		
		return sb.toString();
	}
	
	public static String getHtmlFromFile(String filename) {
		StringBuilder sb = new StringBuilder();
		
		BufferedReader lineReader = null;
		FileReader fr = null;
		
		try {
			fr = new FileReader(filename);
			lineReader = new BufferedReader(fr);
			
			String line;
			while ((line = lineReader.readLine()) != null) {
				sb.append(line);
				sb.append("\n");
			}
			
			if (fr != null) {
				fr.close();
			}
			
			if (lineReader != null) {
				lineReader.close();
			}
		} catch (FileNotFoundException e) {
			sb.append(renderErrorPage("File not found"));
		} catch (IOException e) {
			sb.append(renderErrorPage("File failed to read"));
		}
		
		return sb.toString();
	}
}