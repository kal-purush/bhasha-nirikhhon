package ae.projecteuler.helper;

/**
 * Palindrome helper.
 */
public class Palindrome {

  private Palindrome() {}

  /**
   * Checks that a number is a palindrome
   * (reads the same in both directions: left-to-right / right-to-left).
   *
   * @param number Number to test if it is a palindrome
   * @return Boolean
   */
  public static Boolean isPalindrome(Long number) {
    String original = number.toString();
    String reversed = new StringBuilder(original).reverse().toString();

    return original.equals(reversed);
  }

}