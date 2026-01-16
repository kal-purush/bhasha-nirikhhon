package csvReader;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;

/**
 *
 * @author NobleSoft
 */
public class CsvReaderMain {

    /**
     * @param args the command line arguments
     * @throws java.io.IOException
     * @throws java.io.FileNotFoundException
     */
    public static void main(String[] args) throws IOException, FileNotFoundException {
        //we declare a buffer reader that will read the csv file and pass the csv file to it...
        BufferedReader csvBR = new BufferedReader(new FileReader("slatecode test.csv"));
        //we declare an instance of the ReadFile class
        ReadFile executeRead = new ReadFile();
        //run the method that reads the csv file and give the declared buffer reader as an argument
        executeRead.readData(csvBR);
    }

}