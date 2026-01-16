package com.waffelmonster.expressionparser;

import com.waffelmonster.expressionparser.conversion.Expression;

import java.util.Scanner;

/**
 * Created by jonnyfrey on 3/1/16.
 */
public class ExpressionParser {


    public static void main(String[] args) {
        String argument = "";
        if (args.length == 0) {
            argument = applyScanner();
        } else {
            argument = concat(args);
        }

        Expression expression = new Expression();
        String[] parsed = new String[]{"Unable", "to", "parse"};
        String result = "Unable to parse";
        try {
            parsed = expression.parse(argument.split(" "));
            result = "" + expression.evaluate(parsed);
        } catch (RuntimeException e) {
            e.printStackTrace();
        } finally {
            System.out.println("Original = " + argument);
            System.out.println("Postfix  = " + concat(parsed));
            System.out.println("Result   = " + result);
        }

    }

    private static String applyScanner() {
        Scanner scanner = new Scanner(System.in);
        System.out.println("Input expression");
        return scanner.nextLine();
    }


    private static String concat(String[] rpn) {
        String result = "";
        if (rpn == null) {
            return result;
        }
        for (String element : rpn) {
            result += element + " ";
        }
        return result.trim();
    }

}