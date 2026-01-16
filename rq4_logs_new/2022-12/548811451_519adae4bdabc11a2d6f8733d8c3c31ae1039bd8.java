package com.kienast.regex;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class RegexChallenges {

    public static void main(String[] args) {
        String c1 = "I want a bike.";
        System.out.println(c1.matches("I want a bike."));

        String regeExp = "I want a \\w+.";
        System.out.println(c1.matches(regeExp));

        String c2 = "I want a ball.";
        System.out.println(c2.matches(regeExp));

        String regeExp2 = "I want a (bike|ball).";
        System.out.println(c1.matches(regeExp2));
        System.out.println(c2.matches(regeExp2));

        String textPattern = "I want a \\w+.";
        Pattern pattern = Pattern.compile(textPattern);

        Matcher matcher = pattern.matcher(c1);
        System.out.println(matcher.matches());
        matcher.reset();

        matcher = pattern.matcher(c2);
        System.out.println(matcher.matches());

        String c4 = "Replace all blanks with underscores.";
        System.out.println(c4.replaceAll("\\s", "_"));

        String c5 = "aaabccccccccdddefffg";
        System.out.println(c5.matches("[abcdefg]+"));
        System.out.println(c5.matches("[a-g]+"));
        System.out.println(c5.matches("^a{3}bc{8}d{3}ef{3}g$"));

        String c6 = "abcd.135";
        String c7 = "f5.12a";
        System.out.println(c6.matches("^[a-zA-Z]+\\.[0-9]+$"));
        System.out.println(c6.matches("^[a-zA-Z]+\\.\\d+$"));
        System.out.println(c7.matches("^[a-zA-z]+\\.[0-9]+$"));

        String c8 = "abcd.135uvqz.7tzik.999";
        Pattern pattern8 = Pattern.compile("[A-Za-z]+\\.(\\d+)");
        Matcher matcher8 = pattern8.matcher(c8);

        while(matcher8.find()) {
            System.out.println("Occurrence: " + matcher8.group(1));
        }

        String c9 = "abcd.135\tuvqz.7\ttzik.999\n";
        Pattern pattern9 = Pattern.compile("[A-Za-z]+\\.(\\d+)\\s");
        //Matcher matcher9 = pattern9.matcher(c9.replaceAll("\\s", ""));
        Matcher matcher9 = pattern9.matcher(c9);

        while(matcher9.find()) {
            System.out.println("Occurrence: " + matcher9.group(1));
        }

        String c10 = "abcd.135\tuvqz.7\ttzik.999\n";
        Pattern pattern10 = Pattern.compile("[A-Za-z]+\\.(\\d+)\\s");
        Matcher matcher10 = pattern10.matcher(c10);

        while(matcher10.find()) {
            System.out.println("Occurrence: start = " + matcher10.start(1) + "; end = " + (matcher10.end(1) - 1));
        }

        String c11 = "{0, 2}, {0, 5}, {1, 3}, {2, 4}";
        //Pattern pattern11 = Pattern.compile("\\{(.+?)\\}");
        //Pattern pattern11 = Pattern.compile("\\{([0-9]+,\\s[0-9]+)\\}");
        Pattern pattern11 = Pattern.compile("\\{(\\d+, \\d+)\\}");
        Matcher matcher11 = pattern11.matcher(c11);

        while(matcher11.find()) {
            System.out.println("Occurrence: " + matcher11.group(1));
        }

        String c12 = "11111";
        System.out.println(c12.matches("^\\d{5}$"));

        String c13 = "11111-1111";
        System.out.println(c13.matches("^\\d{5}-\\d{4}$"));

        System.out.println(c12.matches("^\\d{5}(-\\d{4})?$"));
        System.out.println(c13.matches("^\\d{5}(-\\d{4})?$"));



    }

}