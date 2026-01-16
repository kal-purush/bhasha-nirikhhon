package ie.atu;
import java.io.File;
import java.io.IOException;
import java.io.FileWriter;

// Press Shift twice to open the Search Everywhere dialog and type `show whitespaces`,
// then press Enter. You can now see whitespace characters in your code.
public class WriteToFile {

    public static void main(String[] args) {
        String fileName = "File13.txt";

        try(FileWriter fileWriter = new FileWriter(fileName, true)){

            fileWriter.write("This is an extra line");

            System.out.println("Data has been printed to the file.");
        }
        catch (IOException e)
        {
            System.out.println("An error occurred while writing to file");
            e.printStackTrace();
        }
    }
}