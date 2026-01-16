package level1;
import java.util.*;
import java.util.regex.Pattern;
import java.util.regex.Matcher;

class Solution {

	public static int row;
	
    public static void main(String args[]) {
        Scanner in = new Scanner(System.in);
        int N = in.nextInt(); // Number of elements which make up the association table.
        int Q = in.nextInt(); // Number Q of file names to be analyzed.
        
        String types[][] = new String[N][2];
        
        for (int i = 0; i < N; i++) {
            String EXT = in.next(); // file extension
            types[i][0] = EXT.toLowerCase();
            
            String MT = in.next(); // MIME type.
            types[i][1] = MT;
            in.nextLine();
            
            System.err.println(EXT + " " + MT+ " "+"Count: " + i);
        }
        for (int i = 0; i < Q; i++) {
            
            String FNAME = in.nextLine(); // One file name per line.
            System.err.println("Input: " + FNAME);
            
            //checking for a dot"." in the filename          
			boolean res = stringContainsItemFromList(FNAME, types);
	        if(res == true){
	            System.err.println("Result: ");
	            System.out.println(types[row][1]);
	        }
	        else if(res == false){
	            System.err.println("Result: ");
	            System.out.println("UNKNOWN");
	        }                        
        }        
    }
    
    public static boolean stringContainsItemFromList(String inputString, String[][] items)
    {
        
        Pattern pattern;
    	Matcher matcher;
    	
        String regex = "\\.[a-zA-Z0-9]+$";
        
        pattern = Pattern.compile(regex);
        matcher = pattern.matcher(inputString);
        
        if(matcher.find()){
            String res = inputString.substring(matcher.start()+1, matcher.end()).toLowerCase();
            System.err.println("Substring: " + res);
            
            for(int i =0; i < items.length; i++)
            {   
                if(res.equals(items[i][0]))
                {
                    row = i;
                    return true;
                }        
            }
        }
        return false;
    }
}