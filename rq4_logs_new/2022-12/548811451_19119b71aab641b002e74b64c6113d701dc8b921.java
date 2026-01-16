package com.kienast.regex;

import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class IntroMain {

    public static void main(String[] args) {
        String string = "I am a string. Yes, I am.";
        System.out.println(string);
        String yourString = string.replaceAll("I", "You");
        System.out.println(yourString);

        String alphanumeric = "abcDeeeF12Ghiiiiiijkl99z";
        System.out.println(alphanumeric.replaceAll(".", "Y"));
        System.out.println(alphanumeric.replaceAll("^abcDeee", "YYY"));
        System.out.println(alphanumeric.replaceAll("jkl99z$", "AAA"));
        System.out.println(alphanumeric.replaceAll("[aei]", "X"));
        System.out.println(alphanumeric.replaceAll("[aei][Fj]", "Z"));

        String alphanumeric2 = "abcDeeeF12GhiiabcDeeeiiiijkl99z";
        System.out.println(alphanumeric2.replaceAll("^abcDeee", "YYY"));
        System.out.println(alphanumeric2.matches("^hello"));
        System.out.println(alphanumeric2.matches("^abcDeee"));
        System.out.println(alphanumeric2.matches("abcDeeeF12GhiiabcDeeeiiiijkl99z"));

        System.out.println("harry".replaceAll("[Hh]arry", "Harry"));

        String alphanumeric3 = "abcDeeeF12Ghiiiiiijkl99z";
        System.out.println(alphanumeric3.replaceAll("[^ej]", "X"));
        System.out.println(alphanumeric3.replaceAll("[abcdef345678]", "X"));
        System.out.println(alphanumeric3.replaceAll("[a-fA-F3-8]", "X"));
        System.out.println(alphanumeric3.replaceAll("(?i)[a-f3-8]", "X"));
        System.out.println(alphanumeric3.replaceAll("[0-9]", "X"));
        System.out.println(alphanumeric3.replaceAll("\\d", "X"));
        System.out.println(alphanumeric3.replaceAll("\\D", "X"));

        String hasWhiteSpace = "I have blanks and\ta tab, and also a new line\n";
        System.out.println(hasWhiteSpace);
        System.out.println(hasWhiteSpace.replaceAll("\\s", ""));
        System.out.println(hasWhiteSpace.replaceAll("\\t", "X"));
        System.out.println(hasWhiteSpace.replaceAll("\\S", "X"));
        System.out.println(hasWhiteSpace.replaceAll("\\w", "X"));
        System.out.println(hasWhiteSpace.replaceAll("\\D", "X"));
        System.out.println(hasWhiteSpace.replaceAll("\\b", "X"));

        String alphanumeric4 = "abcDeeeF12Ghiiiiiijkl99z";
        System.out.println(alphanumeric4.replaceAll("^abcDeee", "YYY"));
        System.out.println(alphanumeric4.replaceAll("^abcDe{3}", "YYY"));
        System.out.println(alphanumeric4.replaceAll("^abcDe+", "YYY"));
        System.out.println(alphanumeric4.replaceAll("^abcDe*", "YYY"));
        System.out.println(alphanumeric4.replaceAll("^abcD[e]{2,5}", "YYY"));
        System.out.println(alphanumeric4.replaceAll("h+i*j", "YYY"));

        StringBuilder htmlText = new StringBuilder("<h1>My Heading</h1>");
        htmlText.append("<h2>Sub-heading</h2>");
        htmlText.append("<p>This is a paragraph about something.</p>");
        htmlText.append("<p>This is another paragraph about something else.</p>");
        htmlText.append("<h2>Summary</h2>");
        htmlText.append("<p>There is the summary.</p>");

        String h2Pattern = ".*<h2>.*";
        Pattern pattern = Pattern.compile(h2Pattern);
        Matcher matcher = pattern.matcher(htmlText);
        System.out.println(matcher.matches());
    }

}