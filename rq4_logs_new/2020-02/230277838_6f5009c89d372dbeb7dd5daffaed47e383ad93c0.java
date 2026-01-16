package ru.epam.javacore.lesson_26_reg_exp.lesson;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.IntStream;

public class RegExp {

    public static void main(String[] args) {
        //a_1_simpleMatch();
        // a_2_demoCharacterClassesMatch();
        //a_3_demoNumericClassesMatch();
        // a_4_demoCharacterClassesWithCondition();
        //  a_5_demoCharacterClassesWithExcludeCondition();


        //    b_1_demoPredefinedClasses();

        //c_1_demoQuantifier_OnceOrNot();
        //c_2_demoQuantifier_ZeroOrMoreTimes();
        //c_3_demoQuantifier_OneOrMoreTimes();
        // c_4_demoQuantifier_ExactlyTimes();


        //d_1_demo_greedy();
        // d_2_demo_reluctant();
        // d_2_demo_possesive();

        demoCaptureGroups();
    }

    private static void a_1_simpleMatch() {
        match("test", Arrays.asList("test_1", "test_2", "test", "test"));
    }

    private static void a_2_demoCharacterClassesMatch() {
        match("[a-zA-Z]", Arrays.asList("a", "b", "C", "D", "5", "6"));
    }

    private static void a_3_demoNumericClassesMatch() {
        match("[0-9]", Arrays.asList("1", "b", "C", "D", "5", "6"));
    }

    private static void a_4_demoCharacterClassesWithExcludeCondition() {
        match("[0-[^9]]", Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9"));
    }

    private static void a_5_demoCharacterClassesWithExcludeCondition() {
        match("[^abc]", Arrays.asList("a", "b", "c", "d", "ee"));
        //match("[[^abc]-z]", Arrays.asList("a", "b", "c", "d", "ee"));
    }

    private static void b_1_demoPredefinedClasses() {
      /*  System.out.println("Demo digital");
        match("\\d", Arrays.asList("1", "2", "c", "d", "3"));
*/

      /*
        System.out.println("Demo white space");
        matchAndPrint("\\s", Arrays.asList(" ", "    ", "c", "d", "3"));
        */

        /*System.out.println("Demo word [a-zA-Z_0-9]");
        match("\\w", Arrays.asList("a", "b", "1", "2", "%", "$"));
        */

        /*
        System.out.println("Demo NONE word ![a-zA-Z_0-9]");
        match("\\W", Arrays.asList("a", "b", "1", "2", "%", "$"));
        */

        System.out.println("Demo any");
        match(".", Arrays.asList("a", "b", "1", "2", "%", "$"));
    }

    private static void match(String regExp, List<String> itemsToMatch) {
        itemsToMatch.forEach(s -> {
            if (s.matches(regExp)) {
                System.out.println(s);
            }
        });
    }

    private static void matchAndPrint(String regExp, List<String> itemsToMatch) {
        itemsToMatch.forEach(s -> {
            if (s.matches(regExp)) {
                System.out.println("Matched! " + "'" + s + "'");
            }
        });
    }

    private static void c_1_demoQuantifier_OnceOrNot() {
        match("abc?", Arrays.asList("ab", "abc", "abcd"));
    }

    private static void c_2_demoQuantifier_ZeroOrMoreTimes() {
        match("Hello (world )*", Arrays.asList("Hello world",
                "Hello",
                "Hello world world",
                "Hello world world2"));
    }

    private static void c_3_demoQuantifier_OneOrMoreTimes() {
        match("Hello (world )+", Arrays.asList("Hello world",
                "Hello",
                "Hello world world",
                "Hello world world2"));
    }

    private static void c_4_demoQuantifier_ExactlyTimes() {
        match("Hello (world ){2}", Arrays.asList("Hello world",
                "Hello",
                "Hello world world ",
                "Hello world world2"));
    }


    private static void d_1_demo_greedy() {
        // demoRegularExpression(".*ivan", "ivan_petr_ivan_fff_ivan2_tt");
        demoRegularExpression("\" .* \"", "\"two\" three \"four\"");
    }

    //неохотный
    private static void d_2_demo_reluctant() {
        demoRegularExpression(".*?ivan", "ivan_petr_ivan_fff_ivan2_tt");
    }

    //Super greedy (Possessive)
    //demoQuantifier(".*+ivan", "ivan_petr_ivan_fff_ivan2_tt");
    //1 - ivan_petr_ivan_fff_ivan2_tt
    //2 - ivan_petr_ivan_fff_ivan2_tt_ivan
    //demoQuantifier(".*+", "ivan_petr_ivan_fff_ivan");

    /**
     * The main practical benefit of possessive quantifiers is to speed up your regular expression.
     * In particular, possessive quantifiers allow your regex to fail faster. In the above example,
     * when the closing quote fails to match, we know the regular expression couldn’t possibly have skipped over a quote.
     */
    private static void d_2_demo_possesive() {
        demoRegularExpression(".*+ivan", "ivan_petr_ivan_fff_ivan2_tt_ivan");
        // demoRegularExpression(".*+", "ivan_petr_ivan_fff_ivan");


    }

    private static void demoRegularExpression(String regExp, String toMatch) {
        Pattern pattern = Pattern.compile(regExp);
        Matcher matcher = pattern.matcher(toMatch);

        boolean found = false;

        while (matcher.find()) {
            System.out.printf("I found the text \"%s\" starting at index %d and ending at index %d.%n",
                    matcher.group(),
                    matcher.start(),
                    matcher.end());
        }
    }


    private static void demoCaptureGroups() {
        String s = "m 234 tt 47 RUS";
        String regExp = "([a-z]{1}) (\\d{3}) ([a-z]{2}) (\\d{2,3}) RUS";
        Pattern pattern = Pattern.compile(regExp);


        Matcher matcher = pattern.matcher(s);

        if (matcher.find() && matcher.groupCount() == 4) {
            IntStream.rangeClosed(1, matcher.groupCount())
                    .forEach(i -> {
                        System.out.println(matcher.group(i));
                    });
        }
    }

}