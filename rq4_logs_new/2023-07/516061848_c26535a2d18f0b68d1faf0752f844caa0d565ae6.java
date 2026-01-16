package Easy.LongestCommonPrefix;

import java.util.Arrays;

public class Solution_Old {

    public String longestCommonPrefix(String[] strs) {
        String result = "";
        int[] lenghtArray = new int[strs.length];

        for (int i = 0; i < strs.length; i++) {
            lenghtArray[i] = strs[i].length();
        }

        System.out.println(Arrays.toString(lenghtArray));

        int max = lenghtArray[0];
        for (int i = 1; i < lenghtArray.length - 1; i++) {
            if (max < lenghtArray[i]) {
                max = lenghtArray[i];
            }
        }

        System.out.println("max==" + max);

        char[] prefix = new char[max];
        int count = 0;

        char[] buffer = strs[0].toCharArray();


        for (int i = 0; i < buffer.length-1; i++) {
            char letter = strs[i].charAt(i);
            if (buffer[i] == letter) {
                count++;
            }
        }






            result = Arrays.toString(prefix);
            return result;
        }


        public static void main (String[]args){
            Solution_Old solution = new Solution_Old();

            String[] array = {"flower", "flow", "flight"};

            System.out.println(solution.longestCommonPrefix(array));
            System.out.println(solution.longestCommonPrefix(array));
        }


    }