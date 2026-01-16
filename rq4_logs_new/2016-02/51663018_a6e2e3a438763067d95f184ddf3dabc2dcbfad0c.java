// Eli F.
// Section: B
// Assignment 3
// Description: Uses the affine cipher and ROT13 to encrypt and then decrypt an inputed string
// Class name: Cipher
// Version 1.0
// 10/2/15

import java.util.*;

public class Cipher {
   
   /**
   * Length of alphabet the user will be using
   */
   public static final int ALPHABET = 26;
   
   /**
   * Encrypts and then decrypts an inputed string using ROT13 and affine cipher
   * will only encrypt letters, special symbols will be ignored
   *
   * @param args user input from the console
   */
   public static void main(String[] args) {
      //creates new scanner object
      Scanner console = new Scanner(System.in);
      
      //gathers input values for 'a', 'b' and the phrase that will be encoded and decoded
      explainer();
      String aa = prompt(console,"Please enter an odd numeric value for a (1-25, not 13): ","int");
      //coverts aa to int to be used for encoding and checks to make suer it is valid
      int a = verifies(console, Integer.parseInt(aa), 'a'); 
      String bb = prompt(console, "Please enter a numeric value for b: ", "int");
      //converts bb to int to be used for encoding and checks to make sure it is valid
      int b = verifies(console, Integer.parseInt(bb), 'b');
      console.nextLine();
      String phrase = prompt(console, "Enter the phrase you'd like to convert: ", "string");
      
      //prints results of encrypting and decrypting with ROT13 and affine cipher
      results(affineC(a, b, phrase), decryptAffine(a, b, affineC(a, b, phrase)), phrase, "Affine");
      results(rot13(phrase), rot13(rot13(phrase)), phrase, "ROT13");
   }
   
   /**
   * Prints an explanation of what the program will do
   */
   public static void explainer() {
      System.out.println("This program will encode and then decode");
      System.out.println("a phrase using the ROT13 and Affine Ciphers.");
      System.out.println("An Affine Cipher encodes character with an equation");
      System.out.println();
      System.out.println("(ax + b) mod 26");
      System.out.println();
      System.out.println("where x is the ASCII value of the character.");
      System.out.println("ROT13 is a special case of the Affine Cipher,");
      System.out.println("where a is 1 and b is 13.");
      System.out.println();
   }
   
   /**
   * Prompts the user for an integer
   *
   * @param input scanner object used to get input from user
   * @param question the question that will prompt the user
   * @param intOrLetter indicates if int or string is to be collected from user
   * @return userInput the int or string the user inputed in a String
   */
   public static String prompt(Scanner input, String question, String intOrLetter) {
      System.out.print(question);
      //this is to have only one method to prompt for both strings and ints
      //if intOrLetter = "string" then scanner will accept string if not then it will accept int
      String userInput = (intOrLetter.equals("string")) ? input.nextLine()
      :Integer.toString(input.nextInt());
      //returns strings which will be converted into int if need be in main
      return userInput;
   }
   
   /**
   * Verifies if user inputed valid value for 'a'
   * if value was invalid will re-prompt for a new value for 'a'
   *
   * @param input scanner object used to collect user input
   * @param test the value that will be tested
   * @param aOrB indicated if 'a' or 'b' is to be tested
   * @return test new 'a' or 'b' or the original value of 'a' or 'b' if it was valid
   */
   public static int verifies(Scanner input, int test, char aOrB) {
      if(aOrB == 'a') { //if were checking 'a'
         //if an invalid 'a' was entered displays and error message and terminates the program
         if(test%2 == 0 || test > ALPHABET || test == 13) {
            System.out.print("The value for 'a' must be an odd number "); 
            System.out.println("between 1 and 25 not including 13");
            System.exit(0);
         }
         //extra re-prompt feature 
         //  while(test%2 == 0 || test > ALPHABET || test == 13) {                                  
         //     System.out.println("The value for 'a' must be an odd number");
         //     System.out.print("Please enter a new value of a: ");
         //     test = input.nextInt();
         // }
      return test; //doesn't actually do anything unless re-prompt used 
      }
      else {
         //if invalid 'b' was entered displays error message and terminates program
         if(test < 1 || test >= ALPHABET) {
            System.out.println("The value for 'b' must be a number between 0 and 26");
            System.exit(0);
         }
         //extra re-prompt feature 
         // while(b < 1 || b >= ALPHABET) {
         //    System.out.println("The value for 'b' must be a number between 0 and 26");
         //    System.out.print("Please enter a new value for b: ");
         //    test = input.nextInt();
         // }
      return test; //doesn't actually do anything unless re-prompt used 
      }
   }
   
   /**
   * Prints the encoded and decoded phrase using affine cipher
   * also prints the result of a test which compares the decode phrase with the original
   *
   * @param result result of applying the affine cipher to the original phrase
   * @param decryptedPhrase result of applying decryption equation to the decrypted phrase
   * @param originalphrase the original phrase that will be compared to the decrypted phrase
   * @param ciphertype the name of the cipher that will be used to encrypt
   */
   static void results(String encoded, String decoded, String initial, String ciphertype) {
      System.out.println();
      System.out.println("The " + ciphertype +" encoded phrase is: "+ encoded);
      System.out.println("The " + ciphertype +" decoded phrase is: "+ decoded);
      succesTest(initial, decoded);
   }
   
   /**
   * Tests to see if phrases were correctly encrypted and decrypted
   *
   * @param originalPhrase the original phrase the user inputed
   * @param decryptedPhrase the phrase which has been encrypted and then decrypted
   */
   public static void succesTest(String originalPhrase, String decryptedPhrase) {
      String test = (originalPhrase.equals(decryptedPhrase)) ? "The conversion worked correctly."
      :"The conversion did not work";  //sorry for weird line break trying have lines under 100char
      System.out.println(test);
   }
   
   /**
   * Check to see if a character is upper case or lower case
   *
   * @param character the character that will be tested
   * @return letter first letter of the alphabet depending on the case of the character
   * if character is not a letter then returns character
   */
   public static char caseCheck(char character) {
      if(character <= 'Z' && character >= 'A') {
         char letter = 'A';
         return letter;
      }
      else {
         if(character <= 'z' && character >= 'a') {
            char letter = 'a';
            return letter;
         }
         else { //if special character ignore
            char letter = character;
            return letter;
         }
      }
   }
   
   /**
   * Encrypts inputed phrase using ROT13, and also decrypts inputed phrase encrypted using ROT13
   * this is possible because ROT13 is its own inverse since the our alphabet is 26 character long
   *
   * @param phrase the phrase that will be encrypted or decrypted using ROT13
   * @return encryptedPhrase the encrypted phrase or decrypted phrase depending on the input
   */
   public static String rot13(String phrase) {
      int shiftFactor = 13; //rot13 means rotate/shift 13 spaces
      String encryptedPhrase = "";
      for(int ii = 0; ii < phrase.length(); ii++) {
         //grab characters from entered string to encrypt
         char character = phrase.charAt(ii);
         int letterCase = caseCheck(character);
         if(letterCase == 'A' || letterCase == 'a') {
            int index = ((character - letterCase + shiftFactor) % ALPHABET + letterCase);
            encryptedPhrase += (char) index;
         }
         else {
            encryptedPhrase += character; //if the character is a special symbol ignore
         }
      }
      return encryptedPhrase;
   }
   
   /**
   * Encrypts inputed phrase using a affine cipher
   *
   * @param a the multiplicative factor used for encrypting
   * @param b the shift factor used for encrypting
   * @param phrase the phrase that will be encrypted using affine cipher
   * @return encryptedPhrase the phrase encrypted using affine cipher
   */
   public static String affineC(int a, int b, String phrase) {
      String encryptedPhrase = "";
      for(int ii = 0; ii < phrase.length(); ii++) {
         //grab characters from entered string to encrypt
         char character = phrase.charAt(ii);
         int letterCase = caseCheck(character);
         if(letterCase == 'A' || letterCase == 'a') {
            //equation to encrypt
            int index = ((a * (character - letterCase) + b) % ALPHABET + letterCase); 
            encryptedPhrase += (char) index;
         }
         else {
            encryptedPhrase += character; //if the character is a special symbol ignore
         }
      }
      return encryptedPhrase;
   }
   
   /**
   * Finds modular multiplicative inverse of 'a'.  it must satisfy the equation a*a^-1 mod m = 1
   * also first factor of a^-1 will be within range 1-25 so we can search for a^-1
   *
   * @param a the multiplicative factor used to find the modular multiplicative inverse
   * @return aPrime modular multiplicative inverse of 'a'
   */
   public static int aPrime(int a) {
      int aPrime = 0;
      for(int ii = 0; ii <= ALPHABET; ii++) {
         if((ii * a) % ALPHABET == 1) {
            aPrime = ii;
         }
      }
      return aPrime;
   }

   /**
   * Decrypts inputed phrase encrypted by a affine cipher
   *
   * @param a the multiplicative factor used for encrypting
   * @param b the shift factor used for encrypting
   * @param phrase the phrase that will be decrypted
   * @return decryptedPhrase the result of applying the decryption algorithm to an encrypted phrase
   */
   public static String decryptAffine(int a, int b, String phrase) {
      String decryptedPhrase = "";
      for(int ii = 0; ii < phrase.length(); ii++) {
         char character = phrase.charAt(ii);
         int letterCase = caseCheck(character);
         if(letterCase == 'A' || letterCase == 'a') {
            int index = aPrime(a) * (character - letterCase - b);
            //decryption equation
            index = (index < 0) ? (ALPHABET+index%ALPHABET+letterCase):(index%ALPHABET+letterCase);
            decryptedPhrase += (char) index;
         }
         else {
            decryptedPhrase += character; //if the character is a special symbol ignore
         }
      }
      return decryptedPhrase;
   }
}