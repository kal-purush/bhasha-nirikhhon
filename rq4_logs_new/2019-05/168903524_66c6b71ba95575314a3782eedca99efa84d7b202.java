// Vincent Nguyen
// This is a simple calculator which can do the 4 basic math operations and a powers operation.
// I will continue to add more functionality as I continue working on it.
import java.io.IOException;
import java.util.InputMismatchException;
import java.util.Properties;
import java.util.Scanner;

public class CalculatorJav {

  // Scope refers to variables and methods from one part of a program to another
  // A variable used to store info and can be referenced in a program

  /*
   * This calculator uses int, double, String, boolean. the int is used for the case code when
   * choosing an operation double is the input, the number that is to be calculated string is to
   * make a statement then the answer to save time. string is also used as the input type so the
   * boolean function can work. boolean here is used to check whether the input is a number.
   */

  /*
   * the == works by comparing object references, meaning the same object and not something that is
   * actually equal. Strings need to use compareTo or equals method for that type
   */


  /*
   * Operator precedence is basically java having operators like +, -, *, /, etc being done in order
   * of what java thinks is important. a * will have precedence over + in java and will be done
   * first.
   */

  /*
   * Inheritance is a function that allows a class in java access another class's fields and
   * methods. This allows for code reusability because you don't need to type out the code again and
   * it shows a kind of organized structure to help out coding.
   */

  /*
   * Using break in a loop makes the loop head to the exit condition. Using continue just makes the
   * loop jump to the next iteration.
   */

  public static void main(String[] args) throws IOException {
    String num1;
    String num2;
    double realNum1;
    double realNum2;
    boolean getMeOut = true;
    @SuppressWarnings("resource")
    Scanner input = new Scanner(System.in);
    System.out.println("Hello Human! \n");
    Properties prop = new Properties();
    prop.load(
        CalculatorJav.class.getClassLoader().getResourceAsStream("messages/messages.properties"));
    do {
      System.out.println(prop.getProperty("calculator.input.message"));
      // final means that value cannot be changed. This means that stateAns is always "The Answer is
      // "
      num1 = input.nextLine();
      num2 = input.nextLine();
      try {
        if (InputCheckerValidator.realNumber(num1) == true
            && InputCheckerValidator.realNumber(num2) == true) {
          realNum1 = Double.parseDouble(num1);
          realNum2 = Double.parseDouble(num2);
          System.out.println(prop.getProperty("calculator.operator.message"));
          int choose = input.nextInt();
          
          // the array has the parsed numbers in itself
          double arr[] = {realNum1, realNum2};
          double nthTerm = arr.length;
          
          OperationCalculatorClass op = new OperationCalculatorClass(realNum1, realNum2);
          switch (choose) {
            case 1:
              // addOper is a method call. realNum1 and realNum2 are the arguments.
              System.out.println(op.addOper(realNum1, realNum2));
              break;
            case 2:
              System.out.println(op.subOper(realNum1, realNum2));
              break;
            case 3:
              System.out.println(op.mulOper(realNum1, realNum2));
              break;
            case 4:
              OperationCalculatorClass Remain = new RemainderClass(realNum1, realNum2);
              System.out.println(
                  op.divOper(realNum1, realNum2) + " " + Remain.divOper(realNum1, realNum2));
              break;
            case 5:
              System.out.println(op.powOper(realNum1, realNum2));
              break;
            case 6:
              System.out.println(op.randOper(realNum1, realNum2));
              break;
            case 7:
              System.out.println(op.factOper(realNum1));
              break;
            case 8:
              System.out.println(op.varOp(arr, nthTerm) + " "
                  + op.stdDev(arr, nthTerm));
              break;
            case 9:
              getMeOut = false;
              break;
            default:
              // defaults only when a number that is not the case list.
              System.out.println("Illegal Operation");
              break;
          }
        } else {
          System.out.println("The inputs are invalid, please don't use characters or symbols.\n"
              + "Although, negative numbers can be represented via -4 or -20.");
        }


      } catch (InputMismatchException iem) {
        System.out.println("Invalid Input \n"
            + "This includes characters with numbers or symbols with numbers I.E. 5^ or 2x");
      } catch (Exception e) {
        System.out.println("Please contact me at 239-848-0059");
      }
    } while (getMeOut);

  }
}