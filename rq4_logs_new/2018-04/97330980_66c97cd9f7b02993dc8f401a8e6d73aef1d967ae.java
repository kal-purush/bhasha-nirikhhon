package JavaCoding.strings;

import java.util.Scanner;

/**
 * Created by pambast on 8/2/17.
 *
 * toggle the character of string into lowercase or uppercase
 *
 * Input:
 * qweRTyuiPO
 *
 * Output:
 * QWErtYUIpo
 *
 */
public class stringToggler {

    public static void main (String [] args){

        Scanner sc = new Scanner(System.in);
        String input = sc.next();
        String output = "";

        for (char c: input.toCharArray()) {
            int ascii = (int)c;
            if (ascii >= 97 && ascii<=122){
                ascii -= 32;
                c= (char) ascii;
            }else{
                ascii += 32;
                c= (char) ascii;
            }
            output += c;
        }
        System.out.print(output);
    }

}