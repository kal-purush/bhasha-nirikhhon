package compiler;

import java.io.IOException;
import java.io.FileOutputStream;

/**
 * Credit: Dr. Robert Kelly
 */

public class Utilities{

    /**
     * Writes a byte array to a file
     * @param bytearray the array of bytes to write
     * @param fileName the name of the file to write the bytes to
     */
    public static void writeFile(byte[] bytearray, String fileName){

        try{
            FileOutputStream out = new FileOutputStream(fileName);
            out.write(bytearray);
            out.close();
        }
        catch(IOException e){
        System.out.println(e.getMessage());
        }
        
    }//end writeFile

}//end class    