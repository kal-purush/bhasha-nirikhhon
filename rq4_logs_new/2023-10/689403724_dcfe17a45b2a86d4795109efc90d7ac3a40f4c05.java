import java.util.HashMap;
import java.util.Map;
import java.util.Scanner;

public class MorseCodeHashMap {
    static Map<Character, String> morseCodeMap = new HashMap<>();
    static String mrs="A.-\r\n"
        + "B-...\r\n"
        + "C-.-.\r\n"
        + "D-..\r\n"
        + "E.\r\n"
        + "F..-.\r\n"
        + "G--.\r\n"
        + "H....\r\n"
        + "I..\r\n"
        + "J.---\r\n"
        + "K-.-\r\n"
        + "L.-..\r\n"
        + "M--\r\n"
        + "N-.\r\n"
        + "O---\r\n"
        + "P.--.\r\n"
        + "Q--.-\r\n"
        + "R.-.\r\n"
        + "S...\r\n"
        + "T-\r\n"
        + "U..-\r\n"
        + "V...-\r\n"
        + "W.--\r\n"
        + "X-..-\r\n"
        + "Y-.--\r\n"
        + "Z--..\r\n"
        + "0-----\r\n"
        + "1.----\r\n"
        + "2..---\r\n"
        + "3...--\r\n"
        + "4....-\r\n"
        + "5.....\r\n"
        + "6-....\r\n"
        + "7--...\r\n"
        + "8---..\r\n"
        + "9----.\r\n"
        + "..-.-.-\r\n"
        + ",--..--\r\n"
        + "?..--..\r\n"
        + "--....-\r\n"
        + "/-..-.\r\n"
        + ":---...\r\n"
        + "'.----.\r\n"
        + "--....-\r\n"
        + ")-.--.-\r\n"
        + ";-.-.-.\r\n"
        + "(-.--.\r\n"
        + "=-...-\r\n"
        + "@.--.-.\r\n"
        + "!-.-.--\r\n"
        + "&.-...\r\n"
        + "$...-..-\r\n"
        + "\".-..-.\r\n"
        + "_..--.-\r\n"
        + "--....-\r\n"
        + "+.-.-.";
    public static void main(String[] args) {
        
        Scanner getData = new Scanner(System.in);
        String[] lines = mrs.split("\n");
        for (String line : lines) {
            char key = line.charAt(0);           
            String value = line.substring(1);
            morseCodeMap.put(key, value); 
        }
            System.out.println("Enter message to see its morse Code");
            String message = getData.nextLine();
            message=message.toUpperCase();
            String res="";
            for(int i=0;i<message.length();i++)
            {
                try {
                    if(message.charAt(i)==' '){
                        res=res.concat("     ");
                        continue;
                    }
                    String tmp=morseCodeMap.get(message.charAt(i));
                    res+=tmp.substring(0,tmp.length()-1)+" ";
                } catch (Exception e) {
                    res=" unable to find mrse code for "+message.charAt(i);
                    break;
                }
            }
            System.out.println("Morse codes are taken from external site it might be wrong");
            System.out.println(res);
        getData.close();
    }
}