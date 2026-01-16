package salia.week6;

public class RemovingDuplicates {

    public static void main(String[] args) {

        String str = "AAABBBCCC";
        System.out.println("removeDuplicate(str) = " + removeDuplicate(str));

    }


    private static String removeDuplicate(String str) {
        String result = "";

        for (int i = 0; i < str.length(); i++) {
            String ch = "" + str.charAt(i);      // char ch=str.charAt(i);
            if (result.contains(ch)) {
                continue;
            }
            result += ch;
        }
        return result;
    }
}

