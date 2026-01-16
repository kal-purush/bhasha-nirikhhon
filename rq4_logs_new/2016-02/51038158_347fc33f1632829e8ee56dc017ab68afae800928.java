import java.io.*;
import java.util.*;

public class Solution {

    public static void main(String[] args) {
        /* Enter your code here. Read input from STDIN. Print output to STDOUT. Your class should be named Solution. */
        Scanner in = new Scanner(System.in);
        
        int tests = in.nextInt();
        long n;
        int res;
        for (int t = 0; t < tests; t++) {
            n = in.nextLong();
            res = getDivDigits(n);
            System.out.println(res);
        }
    }
    
    private static int getDivDigits(long n) {
        int count = 0;
        long original = n;
        
        int digit;
        while (n != 0) {
            digit = (int) (n % 10);
            if (digit != 0 && original % digit == 0) {
                count++;
            }
            
            n = n / 10;
        }
        
        return count;
    }
}