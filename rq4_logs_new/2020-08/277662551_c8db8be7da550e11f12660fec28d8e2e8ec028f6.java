package me.drison64.mypin.Utils;

public class StringStitcherUtils {

    public static String stitch(String[] input) {
        return doStitch(input, 0);
    }

    public static String stitch(String[] input, Integer startIndex) {
        return doStitch(input, startIndex);
    }

    private static String doStitch(String[] input, Integer startIndex) {
        String output = "";
        for (int i = startIndex; i < input.length; i++) {
            output = output + input[i];
        }
        return output.substring(1, output.length());
    }

}