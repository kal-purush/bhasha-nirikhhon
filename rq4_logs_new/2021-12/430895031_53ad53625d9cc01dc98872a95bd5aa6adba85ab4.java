import java.io.*;
import java.util.*;
import java.math.BigInteger;

public class RSADecrypt{
    public static void main(String[] args) throws FileNotFoundException {
        // File should be located in the same folder for the java code to be able to 
        // read the file.
        File file = new File("test.enc");
        File fileout = new File("test.dec");
        File dec = new File("pri_key.txt");
        
        BigInteger d = new BigInteger("0");
        BigInteger n = new BigInteger("0");
        PrintWriter lic = new PrintWriter(fileout);
        
        
        
        if (args.length == 2) {
        	file = new File(args[0]);
        	dec = new File(args[1]);
        }
        
        try(Scanner sc = new Scanner(dec))
        {
        	String temp = sc.nextLine();
        	String[] arroftemp = temp.split(" ");
        	d = new BigInteger(arroftemp[arroftemp.length-1]);
        	
        	
        	temp = sc.nextLine();
        	arroftemp = temp.split(" ");
        	n = new BigInteger(arroftemp[arroftemp.length-1]);
        	
        }   catch (IOException e){
            e.printStackTrace();
        }

        
        
        /*try(FileReader fr = new FileReader(file))
        {
            // Content collects the characters in the file char by char
            // i helps break up the files by 3 bits or 3 characters.
        	String block ="";
        	BigInteger p= new BigInteger("0");;
            int content;
            int i = 0;
            while((content = fr.read()) != -1){
            	content = Integer.parseInt(Character.toString((char)content));
                if((i % 6) == 0 && (i != 0)){
                	System.out.println(" : " + i);
                	BigInteger c = new BigInteger(block);
                	p = c.modPow(d, n);
                	System.out.println(p);
                	String temp = Integer.toString(p.intValue());
                	if (temp.length() == 1) {
                		temp = "0"+temp;
                	}
                	
                	for (int ii = 0 ; ii<temp.length()-1; ii=ii+2) {
                		String temp2 = codeToText(Character.toString(temp.charAt(ii))) + codeToText(Character.toString(temp.charAt(ii+1)));
                		System.out.println(temp2);
                		lic.print(temp2);
                	}
                	block = "";
                }
                
                    // adding (char) next to content helps the variable be read as a character
                    //System.out.print((char)content);
                    System.out.print(content + ",");
                	block += content;
                    i++;
            }
        }   catch (IOException e){
            e.printStackTrace();
        }
        
        lic.close();*/
        
        
        
        try(Scanner fr = new Scanner(file))
        {
            // Content collects the characters in the file char by char
            // i helps break up the files by 3 bits or 3 characters.
        	String block ="";
        	BigInteger p= new BigInteger("0");;
        	
            while(fr.hasNextLine()){
            		block = fr.nextLine().trim();
                	BigInteger c = new BigInteger(block);
                	p = c.modPow(d, n);
                	System.out.println(p);
                	
                	
                	String temp = p.toString();
                	if (temp.length() < 6) {
                		temp = "0"+temp;
                	}
                	if (temp.length() < 6) {
                		temp = "0"+temp;
                	}
                	if (temp.length() < 6) {
                		temp = "0"+temp;
                	}
                	if (temp.length() < 6) {
                		temp = "0"+temp;
                	}
                	if (temp.length() < 6) {
                		temp = "0"+temp;
                	}
                	if (temp.length() < 6) {
                		temp = "0"+temp;
                	}

                	
                	for (int ii = 0 ; ii<temp.length()-1; ii=ii+2) {
                		String temp2 = codeToText(Character.toString(temp.charAt(ii)) + Character.toString(temp.charAt(ii+1)));
                		System.out.println(temp2);
                		lic.print(temp2);
                	}
                	block = "";
                }
            }
        
        lic.close();
        
    }
    
    public static String codeToText(String a) {
    	String output = "";
    	switch(a){
    		case "00": case "0":
				output = "a";
				break;
    		case "01": case "1":	
    			output = "b";
    			break;
    		case "02": case "2":
				output = "c";
				break;
    		case "03": case "3":
    			output = "d";
    			break;
    		case "04": case "4":
				output = "e";
				break;
    		case "05": case "5":
    			output = "f";
    			break;
    		case "06": case "6":
				output = "g";
				break;
    		case "07": case "7":
    			output = "h";
    			break;
    		case "08": case "8":
				output = "i";
				break;
    		case "09": case "9":
    			output = "j";
    			break;
    		case "10":
				output = "k";
				break;
    		case "11":
    			output = "l";
    			break;
    		case "12":
				output = "m";
				break;
    		case "13":
    			output = "n";
    			break;
    		case "14":
				output = "o";
				break;
    		case "15":
    			output = "p";
    			break;
    		case "16":
				output = "q";
				break;
    		case "17":
    			output = "r";
    			break;
    		case "18":
				output = "s";
				break;
    		case "19":
    			output = "t";
    			break;
    		case "20":
				output = "u";
				break;
    		case "21":
    			output = "v";
    			break;
    		case "22":
				output = "w";
				break;
    		case "23":
    			output = "x";
    			break;
    		case "24":
				output = "y";
				break;
    		case "25":
    			output = "z";
    			break;
    		case "26":
				output = " ";
				break;
    		default:
    			break;
    	}
    	
    	
    	return output;
    }
}