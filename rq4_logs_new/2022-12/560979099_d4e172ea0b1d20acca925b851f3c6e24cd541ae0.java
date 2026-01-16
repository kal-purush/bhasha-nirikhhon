import com.sun.source.tree.WhileLoopTree;

import java.io.*;
import java.util.ArrayList;
import java.util.HashMap;

public class LZW {
    public HashMap<Integer,String> DecompDictionary = new HashMap<>();
    public ArrayList<Integer> A = new ArrayList<>();
    public  ArrayList<Integer> Compress(String text){
        int dictionarySize = 128;
        HashMap<String,Integer> dictionary = new HashMap<>();
        for (int i = 0; i < 128; i++) {
            dictionary.put((""+(char) i), i);///A=65 , B=66 ,AB =128
            DecompDictionary .put(i ,(""+(char) i));///65=A,66=B
        }
        ArrayList<Integer> ArrOfTags = new ArrayList<>();
        String charr = "";
        for (char c : text.toCharArray()) {
            String newWord = charr + c;/// AB
            if (dictionary.containsKey(newWord))
                charr = newWord;
            /// charr = A
            else {
                ArrOfTags.add(dictionary.get(charr));/// 65 ,
                dictionary.put(newWord, dictionarySize);/// AB = 128
                DecompDictionary.put(dictionarySize,newWord);///128 = AB
                dictionarySize++;
                charr = "" + c;
            }
        }
        if (!charr.equals(""))
            ArrOfTags.add(dictionary.get(charr));
        A = ArrOfTags;

        return ArrOfTags;
    }
    public String Decompress(ArrayList<Integer> A){
        String s = "";
        for (int i=0;i<A.size();++i) {
            s += DecompDictionary.get(A.get(i));///ABA
        }
        return s;
    }
    public static void main(String []Args){
        LZW LZ = new LZW();
        try {
            String str ="";
            int i;
            FileReader inFile  = new FileReader("input.txt");
            FileWriter outFile = new FileWriter("OutPut.txt");
            while((i = inFile.read())!=-1) {
                str += (char)i;
            }
            double orignalSize = 8 * str.length();
            System.out.println("Compressed Data [Tags]: "+LZ.Compress(str));
            System.out.println("DeCompressed Data : " +   LZ.Decompress(LZ.A));
            outFile.write("Compressed Data [Tags] : "+ LZ.Compress(str)+'\n');
            outFile.write("Decompressed Data : "  +   LZ.Decompress(LZ.A)+'\n');
            double CompressedSize = 8 * LZ.A.size();
            outFile.write("String Length : " + str.length()+" Symbols\n");
            outFile.write("Original Size : " + orignalSize + " bits\n");
            outFile.write("Compressed Size : "+CompressedSize+ " bits\n");
//            outFile.write("Compressed Ratio : "+ (orignalSize/CompressedSize));
            inFile.close();
            outFile.close();
        }
        catch (IOException e) {
            throw new RuntimeException(e);
        }
    }
}
//            System.out.println("Compressed Size : " + CompressedSize + " bits");
//            System.out.println("Compressed Ratio : "+ (orignalSize/CompressedSize));