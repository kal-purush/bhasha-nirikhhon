package LeetCode_Daily;

public class Determine_if_String_Halves_Are_Alike {
    public static void main(String[] args) {

        System.out.println(halvesAreAlike("book"));
        System.out.println(halvesAreAlike2());

    }

    public static boolean halvesAreAlike(String s) {
        s = s.toLowerCase();
        String str1 = s.substring(0, s.length() / 2);
        String str2 = s.substring(s.length() / 2);

        char[] arr1 = str1.toCharArray();
        char[] arr2 = str2.toCharArray();

        int count1 = 0;
        int count2 = 0;

        for (int i = 0; i < arr1.length; i++) {
            if (arr1[i] == 'a' || arr1[i] == 'e' || arr1[i] == 'i' || arr1[i] == 'o' || arr1[i] == 'u') {
                count1++;
            }
            if (arr2[i] == 'a' || arr2[i] == 'e' || arr2[i] == 'i' || arr2[i] == 'o' || arr2[i] == 'u') {
                count2++;
            }
        }

        if (count1 == count2) {
            return true;
        } else {
            return false;
        }

    }


    //2nd solution (same but shorter version)
    public static boolean halvesAreAlike2(String s) {

        char[] arr1 = s.substring(0, s.length() / 2).toCharArray();
        char[] arr2 = s.substring(s.length() / 2).toCharArray();

        int count1 = 0;
        int count2 = 0;

        for (int i = 0; i < arr1.length; i++) {

            if (arr1[i] == 'a' || arr1[i] == 'e' || arr1[i] == 'i' || arr1[i] == 'o' || arr1[i] == 'u' ||
                    arr1[i] == 'A' || arr1[i] == 'E' || arr1[i] == 'I' || arr1[i] == 'O' || arr1[i] == 'U') {
                count1++;
            }

            if (arr2[i] == 'a' || arr2[i] == 'e' || arr2[i] == 'i' || arr2[i] == 'o' || arr2[i] == 'u' ||
                    arr2[i] == 'A' || arr2[i] == 'E' || arr2[i] == 'I' || arr2[i] == 'O' || arr2[i] == 'U') {
                count2++;
            }
        }

        if (count1 == count2) {
            return true;
        } else {
            return false;
        }

    }
}