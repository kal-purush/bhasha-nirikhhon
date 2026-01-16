package pack2;

public class Palindrome2 {
	
	
 static boolean isPalindrome(String word){
	 return word.equals(new StringBuilder(word).reverse().toString());
 }
 
 public static void main(String ...args){
	 System.out.println(isPalindrome("ama"));//true
	 System.out.println(isPalindrome("mama"));//false
 }
}