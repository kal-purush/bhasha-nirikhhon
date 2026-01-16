import java.util.Scanner;
import java.util.Arrays;
import java.lang.Math;

public class ArraysLesson {
    public static void main(String[] args) {
        Scanner scanner = new Scanner(System.in);
        String str = scanner.nextLine();
        String[] line = str.split(" ");
        int lowestLength = 0;
        int index = 0;
        for (int i = 0; i < (line.length -1); i++) {
            if (line[i].length() < line[i+1].length()) {
                lowestLength = line[i].length();
            }
        }
        for (int i = 0; i <= (line.length - 1); i++) {
            if(lowestLength == line[i].length()) {
                index = i;
            }
        }
        System.out.println(index+1);
    }
}