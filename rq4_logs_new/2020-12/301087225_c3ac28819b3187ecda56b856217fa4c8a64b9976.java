import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.util.Scanner;
public class Nomor1 {

    public static void main (String[] args) {
        Scanner obj = new Scanner (System.in);
        FileInputStream in = null;
        FileOutputStream out = null;
        String input = obj.next();
        String output = obj.next();
        try {
            in = new FileInputStream("C:\\new\\" + input + ".txt");
            out = new FileOutputStream("C:\\new\\" + output + ".txt");
            int data;
            while ((data = in.read())!= -1){
                out.write(data);
            }
            System.out.println("berhasil");
        } catch (IOException ioe){
            System.out.println("gagal");
        } finally {
            try {
                if (in != null){
                    in.close();
                }
                if (out != null){
                    out.close();
                }
            }
            catch (IOException ioe){
                System.out.println("gagal");
            }
        }
    }
}