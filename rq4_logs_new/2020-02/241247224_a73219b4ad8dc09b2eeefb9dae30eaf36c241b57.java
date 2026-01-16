/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package desencryptedchat;

/**
 *
 * @author Tilley
 */
public class ChatHelper {
    public static String textToBinaryString(String message) {
        byte[] bytes = message.getBytes();
        StringBuilder binary = new StringBuilder();
        for (byte b : bytes) {
            int val = b;
            for (int i = 0; i < 8; i++) {
                binary.append((val & 128) == 0 ? 0 : 1);
                val <<= 1;
            }
            
        }
        String bString = binary.toString();
//        if (bString.length() > 64){
//            System.out.println("!!!MESSAGE IS LONGER THAN 64 BITS!!!");
//        }
        // this is modified
        while (bString.length() % 64 != 0){
            bString = "0" + bString;
        }
        return bString;
        
    }
    
    public static String binaryStringToText(String binaryString){
        String str = "";
        char nextChar;

        for (int i = 0; i <= binaryString.length() - 8; i += 8) 
        {
            nextChar = (char) Integer.parseInt(binaryString.substring(i, i + 8), 2);
            str += nextChar;
        }
        // replaces strange [] character that results from leading zero bytes 
        String outString = str.replaceAll("[\\p{Cc}\\p{Cf}\\p{Co}\\p{Cn}]", "");
        
       
        
        return outString;
    }
    
    // splits text into an array of 64 bit strings
    public static String[] textSplitter(String str){
//        System.out.println("text to split length:" + str.length());
        String[] msgArray = new String[str.length() / 64];
        int index = 0;
        for (int bit = 0; bit <= str.length() - 64; bit += 64){
            String str64bit = str.substring(bit, bit + 64);
            msgArray[index] = str64bit;
            index += 1;
        }
        return msgArray;
    }
    
    public static String keyConverter(String text){
        String semi = textToBinaryString(text);
        String k = semi.substring(semi.length() - 64, semi.length());
        return k;
        
    }
    
   
    
        
    
    
}