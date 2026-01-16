import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Scanner;

public class Main {
    public static void main(String[] args) {
        Scanner sc = new Scanner(System.in);
        while (true) {
            System.out.println(" ------------- ");
            System.out.println("|1.Compress   |");
            System.out.println("|2.Decompress |");
            System.out.println("|3.Exit       |");
            System.out.println(" ------------- ");
            System.out.println("Enter Choice : ");
            int i = sc.nextInt();
            Huffman HuffmanStandard = new Huffman();
            if (i == 1) {
                try {
                    HuffmanStandard.compress("input");
                    HuffmanStandard.GetCompressedAndOriginalSize("input", "inputComPressed");
                } catch (FileNotFoundException e) {
                    System.out.println("File Not found");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            else if (i == 2) {
                try {
                    HuffmanStandard.decompress("input");
                } catch (FileNotFoundException e) {
                    System.out.println("File Not found");
                } catch (IOException e) {
                    e.printStackTrace();
                }
            }
            else if(i == 3) break;
        }
    }
}