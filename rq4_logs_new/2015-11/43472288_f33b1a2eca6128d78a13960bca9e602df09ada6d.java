import edu.sju.SpellChecker;

import java.io.IOException;

public class RunSpellChecker {

    public static void main(String[] args) {
        SpellChecker sc = new SpellChecker();

        try{
            sc.loadDictionary(args[0]);
        } catch (IOException e) {
            System.out.println("Error loading the dictionary file.");
        }

    }
}