// https://www.reddit.com/r/dailyprogrammer/comments/4tetif/20160718_challenge_276_easy_recktangles/

import java.util.Scanner;

public class Solution {

    public static void main(String[] args){
        Scanner scan = new Scanner(System.in);
        String input = scan.nextLine().trim();
        String[] params = input.split("\\s+");

        String word = params[0];
        String[] letters = word.split("");
        int height = Integer.parseInt(params[1]);
        int width = Integer.parseInt(params[2]);

        String fwPattern = getLinePattern(letters[0] + " ", letters, width, true).trim();
        String bwPattern = getLinePattern(letters[letters.length - 1] + " ", letters, width, false).trim();
        String whitespace = getWhiteSpace(letters.length * 2 - 3);

        printVerticalLines(letters, fwPattern, bwPattern, whitespace, height, width);
        System.out.println(bwPattern);
    }

    private static boolean isEven(int n){
        return n % 2 == 0;
    }

    private static String getWhiteSpace(int n){
        String whiteSpace = "";
        for (int i = 0; i < n; i++){
            whiteSpace += " ";
        }
        return  whiteSpace;
    }

    private static String getLinePattern(String beginning,String[] letters, int width, boolean isForward) {
        int modifier = 1;
        if (isForward){
            modifier = 0;
        }

        for (int i = 0 + modifier; i < width + modifier; i++) {
            if (i % 2 == 0) {
                String patternPart = "";
                for (int k = 1; k < letters.length; k++) {
                    patternPart += letters[k] + " ";
                }
                beginning += patternPart;
            } else {
                String patternPart = "";
                for (int k = letters.length - 2; k >= 0; k--) {
                    patternPart += letters[k] + " ";
                }
                beginning += patternPart;
            }
        }
        return beginning;
    }

    private static void printVerticalLines(String[] letters, String fwPattern, String bwPattern, String whitespace,
            int height, int width){
        int modifier = 1;
        if (!isEven(height)){
            modifier = 0;
        }

        for(int i = 0 + modifier; i < height + modifier; i++) {
            if (!isEven(i)) {
                System.out.println(bwPattern);
            } else {
                System.out.println(fwPattern);
            }
            for (int k = 1; k < letters.length - 1; k++) {
                String verticalLetterLine = "";
                for (int j = 0; j < width + 1; j++) {
                    if ((!isEven(i) && isEven(j)) || (isEven(i) && !isEven(j))) {
                        verticalLetterLine += letters[k] + whitespace;
                    } else {
                        verticalLetterLine += letters[letters.length - 1 - k] + whitespace;
                    }
                }
                System.out.println(verticalLetterLine);
            }
        }
    }
}