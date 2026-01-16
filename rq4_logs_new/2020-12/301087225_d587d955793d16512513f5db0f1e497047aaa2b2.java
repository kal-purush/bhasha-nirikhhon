import java.io.*;
import java.util.Scanner;

class Tp8No1 {
    public static void main(String[] args) {
        Scanner user = new Scanner (System.in);
        FileInputStream input = null;
        FileOutputStream output = null;
        try {
            input = new FileInputStream(user.next()+".txt");
            output = new FileOutputStream(user.next()+".txt");
            int file;
            while ((file = input.read()) != -1) {
                output.write(file);
            }
        } catch (IOException ioe) {
            System.out.println("GAGAL");
        }

        finally {
            try {
                if (input != null) {
                    System.out.println("BERHASIL");
                    input.close();
                }
                if (output != null) {
                    output.close();
                }
            } catch (IOException ioe) {
                System.out.println("GAGAL");
            }
        }
    }
}