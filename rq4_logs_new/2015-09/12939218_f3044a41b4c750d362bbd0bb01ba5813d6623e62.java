
/**
 * Model Solution to Homework 1 -- check command-line arguments for palindromes
 * 
 * @author bhoward
 */
public class HW1
{
   /**
    * The starting point for the program. Check each argument from the
    * command-line, and print those that are palindromes to standard output.
    * 
    * @param args
    *           the command-line arguments
    */
   public static void main(String[] args)
   {
      for (int i = 0; i < args.length; i++)
      {
         if (isPalindrome(args[i]))
         {
            System.out.println(args[i]);
         }
      }
   }

   /**
    * Check whether the given string is a palindrome -- that is, it is the same
    * sequence of characters from left to right as from right to left.
    * 
    * @param word
    *           the string to be tested
    * @return true if word is a palindrome
    */
   public static boolean isPalindrome(String word)
   {
      int left = 0, right = word.length() - 1;

      while (left < right)
      {
         if (word.charAt(left) != word.charAt(right))
         {
            // non-matching characters found; not a palindrome
            return false;
         }
         ++left;
         --right;
      }

      // all characters matched left with right; it's a palindrome
      return true;
   }
}