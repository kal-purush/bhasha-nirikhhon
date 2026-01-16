import java.util.Arrays;
import java.util.List;

public class Messy {
    public static void main(String[] args) {
        System.out.println("one");
        System.out.println("two");
        System.out.println("three");
        System.out.println("four");
        System.out.println("five");
/*        this line as well with a hotkey that does multi-line commenting
        Please comment out this line and*/
        List<String> numberWords = Arrays.asList("six", "seven", "eight", "nine");
        for (String numberWord : numberWords) {
//          Use the sout shortcut to write out numberWord;
            System.out.println(numberWord);
        }
    }
}