import java.io.*;
import java.util.*;

/**
 * Created by Kelly on 13/02/2015.
 */
public class FileImporter {
    private List<Word> words = new ArrayList<Word>();
    private Set<String> uniqueWords = new HashSet<String>();
    private List<String> strWords = new ArrayList<String>();

    public static void main(String[] args) throws FileNotFoundException {
        FileImporter f= new FileImporter();
        f.importFile();
    }

    public void importFile() throws FileNotFoundException {
        BufferedReader br = new BufferedReader(new FileReader("src/files/sampleText.txt"));

        try {
            while (br.readLine()!=null){
                StringTokenizer tokens = new StringTokenizer(br.readLine(), " ");
                while(tokens.hasMoreTokens()) {
                    String next = tokens.nextToken();
                    strWords.add(next);
                    uniqueWords.add(next);
                }
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
        List<String> uniqueWordsList = new ArrayList<String>(uniqueWords);

        for (String word : uniqueWordsList){
            Word w = new Word();
            w.setWord(word);
            words.add(w);
        }

        for (String nextWord : strWords){
            checkWord(nextWord);
        }

        for (int i=0; i<words.size(); i++){
            System.out.println(words.get(i).getWord() + "   Count " + words.get(i).getCount());
        }
    }

    //Checks to see if each word is stored in the ArrayList already
    //If stored already, increments counter for that word
    //If not stored, adds that word to the list
    public void checkWord(String token) {
        boolean isStored = false;
        for (int i=0; i<words.size(); i++) {
            if (token.equals(words.get(i).getWord())) {
                words.get(i).increaseCount();
                isStored = true;
            }
        }
        if (!isStored) {
            Word newWord = new Word();
            newWord.setWord(token);
            words.add(newWord);
        }
    }
}