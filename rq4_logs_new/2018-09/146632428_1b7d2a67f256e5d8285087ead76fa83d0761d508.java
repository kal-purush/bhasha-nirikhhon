package FileReaderexample;

import java.io.BufferedReader;
import java.io.FileReader;
import java.nio.Buffer;

public class UseFileReaderBuffer {
    public static void main(String[] args) {
        FileReader fr = null;
        String path = "C:/Users/almrashid/Desktop/test.txt";
        BufferedReader br = null;
        try {
            fr = new FileReader(path);
        } catch (Exception e) {
            System.out.println("file not found");
        }

        br=new BufferedReader(fr);
        String data="";
       try {
           while ((data=br.readLine())!= null){
               System.out.println(data);
           }

           }catch(Exception e1){
               System.out.println("file can not be read");
           }

    }
}