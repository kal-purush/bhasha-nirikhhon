import java.io.File;
import java.io.FileNotFoundException;
import java.util.Scanner;
import java.util.regex.Pattern;
/*
 * Abi Sislo
 * 1444789
 * CS 3861-04
 */
public class Linter {
	public static void main(String [] args){
		Scanner in = new Scanner(System.in);
		System.out.print("Please enter a file name to be read: ");
	    String inputFile= in.next();
	    try{
	      File tempFile = new File(inputFile);
	      Scanner tempScan=new Scanner( tempFile );
	      int ln=1;
	      while(tempScan.hasNextLine()){
	        String line = tempScan.nextLine();
	        okaySemicolonAndBracket(line, ln);
	        okayWhitespace(line, ln);
			okayEquals(line, ln);
			okayApostrophe(line, ln);
	        if(!tempScan.hasNextLine()){
				okayNewLine(line, ln);
			}
	        ln++;
	      }
	      tempScan.close();
	      in.close();
	    } catch (FileNotFoundException e) {
		  System.out.println("File not found");
	    }
	}
	/*public static void linter(String str, int ln){
		okaySemicolon(str, ln);
		okayWhitespace(str, ln);
		okayEquals(str, ln);
		okayApostrophe(str, ln);
	}*/
	public static void okaySemicolonAndBracket(String str, int ln){
		if(str.length()==1 && !str.matches(".*}$")){
			System.out.println(ln + ". A closing brace should stand alone");
		}else if(Pattern.compile(".*"+Pattern.quote(";")+"$").matcher(str).matches()){
			System.out.println(ln + ". The line should end in a semicolon");
		}
	}
	public static void okayBrackets(String str, int ln){
		if(str.length()==1 && str.matches(".*}$")){
			 System.out.println("No syntax errors");
		}else if(str.matches("^\\t*"+Pattern.quote("}")+"$") && str.matches("^\\s*$")){
			System.out.println("No syntax errors");
		}else if(Pattern.compile(".*"+Pattern.quote("{")+"$").matcher(str).matches()){
			System.out.println("No syntax errors");
		} else{
			System.out.println(ln + ". Bracket error");
		}
	}
	public static void okayWhitespace(String str, int ln){
		if(!str.matches("^.*\\s$")){
			System.out.println(ln +". Remove trailing whitespace");
		}
	}
	public static void okayEquals(String str, int ln){
		if(!str.matches(".*==.*") || str.matches(".*===.*")){
			System.out.println(ln + ". Should only use strict equality");
		}else{
			System.out.println("No syntax errors");
		}
	}
	public static void okayApostrophe(String str, int ln){
		if(str.matches(".*\".*\".*")&&str.matches(".*\".*\".*")){
			System.out.println(ln + ". Should use single apostrophe only");
		}
	}
	public static void okayNewLine(String str, int ln){
		if(str.matches("^$")){
			System.out.println("No syntax errors");
		}else{
			System.out.println(ln + ". File doesn't end in a new line");
		}
	}
}