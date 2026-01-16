/**
 * @author joe
 */
public class Client {
    
    // hardcoded strings to test hash code function with
    public static String POTS = "POTS";
    public static String STOP = "STOP";
    public static String TOPS = "TOPS";

    // main client
    public static void main(String[] args) {
        String[] words = {POTS, STOP, TOPS};
        
        for(String str : words)
            System.out.println(str + " : " + cyclicShiftHashCode(str));
        
        // seperation between silent and verbose
        System.out.println("\n\n");
        
        for(String str : words) {
            System.out.println(str + " :");
            int t = verboseCyclicShiftHashCode(str);
            System.out.println("" + t + "\n");
        }
    
        verboseCyclicShiftHashCode("REALLYBIGWORD");
    
    }
    
    // non-verbose cyclic shift hash function
    public static int cyclicShiftHashCode(String str) {
        int h = 0;
        
        // iterate through each character, adding it to the hash code
        for(int i = 0; i < str.length(); i++) {
            h = (h << 5) | (h >> 27);
            h += (int)str.charAt(i);
        }
        
        return h;
    }
    
    // verbose cyclic shift hash function
    public static int verboseCyclicShiftHashCode(String str) {
        int h = 0;
        
        for(int i = 0; i < str.length(); i++) {
            h = (h << 5) | (h >> 27);
            // print out which shift operation this is
            System.out.println("  Shift " + (i+1) + " : " + h);
            
            h += (int)str.charAt(i);
            // print out which addition operation this is
            System.out.println("  Add   " + (i+1) + " : " + h);
        }
        
        return h;
        
    }
    
}