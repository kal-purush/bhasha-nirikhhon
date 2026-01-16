import java.io.*;
import java.util.Scanner;
class Nomor1 {
    public static void main(String[] args) {
        Scanner beby = new Scanner (System.in);
        
        FileInputStream in = null;
        FileOutputStream out = null;
        File isChecked = null;
         

        try {
            String nama = beby.nextLine();
            String nama1 = beby.nextLine();
            beby.close();
            in = new FileInputStream(String.format("%s.txt", nama));
            out = new FileOutputStream(String.format("%s.txt", nama1));
            isChecked = new File(String.format("%s.txt", nama));
            int read;
            while ((read = in.read()) != -1) {
                out.write(read);
            }
            if (isChecked.exists()) {
                System.out.println("berhasil");
            } 
        } catch (IOException ioe) {
            System.out.println("gagal");
        } finally {
            try {
                if (in != null) {
                    in.close();
                }
                if (out != null) {
                    out.close();
                }
                
            } catch (IOException ioe) {
                System.out.println(ioe.getMessage());
            }
        }
    }
}