package ie.atu;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
public class PrintWrite {
    public static void main (String[] args){
        String fileName = "File13.txt";

        try(PrintWriter writer = new PrintWriter(new FileWriter(fileName, true))) {
            writer.println("This line will be appended");
            writer.printf("Appended formatted content: %d %s%n", 42, "example");

            System.out.println("Content successfully appended to the file");
        }
        catch (IOException e) {
            System.out.println("An error occurred while appending to the file.");
            e.printStackTrace();
        }
    }
}