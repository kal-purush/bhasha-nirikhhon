import java.io.BufferedReader;
import java.io.FileReader;
import java.io.IOException;
import java.util.*;

public class CSVReader {

    Arraylist<Pokemon> pokeList = new Arraylist<Pokemon>;

    public static void main(String[] args) {

        String csvFile = "/Users/thuntington/documents/MachineLearning/Saul_Class_Projects/data/project/PokeData.csv";
        String line = "";
        String cvsSplitBy = ",";

        try (BufferedReader br = new BufferedReader(new FileReader(csvFile))) {

            while ((line = br.readLine()) != null) {

                // use comma as separator
                Element[] pokeInstance = line.split(cvsSplitBy);

                Pokemon temp = new Pokemon(pokeinstance[0], pokeInstance[1], pokeInstance[2]);

                pokeList.add(temp);

                System.out.println("Pokemon number= " + pokeInstance[0] + "Pokemon Name= " + pokeInstance[1] + "Occurences: " + pokeInstance[2]);


            }

        } catch (IOException e) {
            e.printStackTrace();
        }

    }

}