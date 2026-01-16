import java.io.*;
import java.util.*;

public class PPNomor1 {
    
    public static void main(String[] args) {
        Scanner iu = new Scanner(System.in);
        FileInputStream in =null;
        FileOutputStream out =null;
        try{
            String input1 = iu.next();
            String input2 = iu.next();
            in= new FileInputStream(input1 + ".txt");
            out = new FileOutputStream(input2 + ".txt");
            int data;
            while((data = in.read()) != -1){
                out.write(data);
            }
        }catch(IOException ioe){
            System.out.println("gagal");
        
        }finally{
             try {
                if(in != null){
                    System.out.println("berhasil");
                    in.close();
                }
                if(out != null){

                    out.close();
                }
            } catch (IOException ioe) {
                System.out.println("gagal");

            }
        }
    }
} 