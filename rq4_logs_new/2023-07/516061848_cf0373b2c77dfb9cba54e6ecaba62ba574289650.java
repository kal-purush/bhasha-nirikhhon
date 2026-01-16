package Easy.LenghtOfTheLastWord;

public class Solution_2 {
    public static int lengthOfLastWord(String s) {
        int count = 0;
        s.trim();

        for (int i = s.length() - 1; i >= 0; i--) {
            if (s.charAt(i) != ' ') {
                count += 1;
            } else {
                System.out.println(count);
                return count;
            }
        }
        System.out.println(count);
        return count;

    }


    public static void main(String[] args) {

        lengthOfLastWord("luffy все еще джойбой");

    }


}