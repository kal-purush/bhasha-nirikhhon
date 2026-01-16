package com.calculator;

import java.util.Scanner;

/**
 * Class for interaction with user
 */
public class Calculator {
  public static void main(String[] args) {
    String answerExit;
    Scanner scanner = new Scanner(System.in);
    do {
      System.out.println("Калькулятор для арифметичних операцій +,-,* та / над римськими " +
          "та арабськими цифрами");
      System.out.print("Введіть числа та операцію над ними та натисніть Enter: ");
      String expression = scanner.nextLine();
      expression = expression.replaceAll(" ", "");
      ExpressionParameters expressionParameters =  ValidatorExpression
          .readNumbersAndOperationsInExpression(expression);
      if (expressionParameters != null) {
        System.out.println("Begin computation");
        Computation.beginComputation(expressionParameters);
      }
      System.out.print("Продовжити обрахунки? Якщо так, то введіть \"yes\", інакше натисніть" +
          " Enter: ");
      answerExit = scanner.nextLine();
    } while (answerExit.equals("yes"));
  }
}