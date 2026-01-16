package com.company;

import java.util.Arrays;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class String3 {

//    Given a string, count the number of words ending in 'y' or 'z' -- so the 'y' in "heavy" and the 'z' in "fez"
//    count, but not the 'y' in "yellow" (not case sensitive). We'll say that a y or z is at the end of a word if there
//    is not an alphabetic letter immediately following it.
    public int countYZ(String str){
        int count = 0;
        String [] words = str.split(" ");
        for(int i =0; i< words.length; i++){
            if(words[i].length() > 0){
                if (words[i].charAt(words[i].length() -1) == 'y' || words[i].charAt(words[i].length() -1) == 'z' )
                    count ++;
            }
        }
        return count;
    }

    //  We'll say that a lowercase 'g' in a string is "happy" if there is another 'g' immediately to its left or right.
//  Return true if all the g's in the given string are happy.
    public boolean gHappy(String word){
        boolean happy = false;
        for(int i = 1; i < word.length() - 1; i++){
            if(word.charAt(i) == 'g'){
                if (word.charAt(i + 1) == 'g' || word.charAt(i - 1) == 'g')
                    happy = true;
                else
                    happy = false;
            }
        }
        return happy;
    }

//  Given a string, return the longest substring that appears at both the beginning and end of the string without
//  overlapping. For example, sameEnds("abXab") is "ab".
    public String sameEnds(String string){
        String maxSub = "";
        for(int i =0; i< string.length(); i++){
            String subs = string.substring(0, i);
            if (string.startsWith(subs) && string.endsWith(subs)){
                if (subs.length() > maxSub.length())
                    maxSub = subs;
            }
        }
        return maxSub;
    }

//  Given a string, return the sum of the numbers appearing in the string, ignoring all other characters.
//  A number is a series of 1 or more digit chars in a row. (Note: Character.isDigit(char) tests if a char is
//  one of the chars '0', '1', .. '9'. Integer.parseInt(string) converts a string to an int.)
    public int sumNumbers(String str){
        String numString = "";
        for(int i =0; i< str.length(); i++){
            if (Character.isDigit(str.charAt(i))){
                numString += str.charAt(i);
            }
            else{
                numString += " ";
            }
        }
        String [] numWords = numString.split(" ");
        int result = 0;
        for(String item : numWords){
            item = item.trim();
            if(item.length() > 0)
                result += Integer.parseInt(item);

        }
        return result;
    }

//  Given two strings, base and remove, return a version of the base string where all instances of the remove string
//  have been removed (not case sensitive). You may assume that the remove string is length 1 or more. Remove only
//  non-overlapping instances, so with "xxx" removing "xx" leaves "x".
    public String withoutString(String a, String b){
        return a.replaceAll(b, "");
    }

//  We'll say that a "triple" in a string is a char appearing three times in a row. Return the number of triples
//  in the given string. The triples may overlap.
    public int countTriple(String string){
        int count = 0;
        for(int i = 0 ; i< string.length() - 2; i++){
            if (string.charAt(i) == string.charAt(i + 1) && string.charAt(i) == string.charAt(i + 2))
                count++;
        }
        return count;
    }

//  Given a string, look for a mirror image (backwards) string at both the beginning and end of the given string.
//  In other words, zero or more characters at the very begining of the given string, and at the very end of the string
//  in reverse order (possibly overlapping). For example, the string "abXYZba" has the mirror end "ab".
    public String mirrorEnds(String str){
        String reverse = "";
        int index = 0;
        for(int i = str.length() - 1; i >= 0 ; i--)
            reverse += str.charAt(i);

        if(str.equals(reverse))
            return str;

        for (int i =0; i< str.length(); i++){
            if (str.charAt(i) != reverse.charAt(i)){
                index = i;
                break;
            }
        }
        return str.substring(0, index);
    }

//  Given a string, return a string where every appearance of the lowercase word "is" has been replaced with "is not".
//  The word "is" should not be immediately preceeded or followed by a letter -- so for example the "is" in "this" does
//  not count. (Note: Character.isLetter(char) tests if a char is a letter.)
    public String notReplace(String str){
        String [] words = str.split(" ");
        String result = "";
        for(String word: words){
            if(word.equals("is"))
                result += " is not ";
            else
                result += word;
        }
        return result.trim();
    }

//  Given a string, return true if the number of appearances of "is" anywhere in the string is equal to the
//  number of appearances of "not" anywhere in the string (case sensitive).
    public boolean equalIsNot(String str){
        int isCount =0, notCount = 0;
        for(int i=0; i< str.length()  - 1; i++){
            if(str.charAt(i) == 'i' && str.charAt(i + 1) == 's')
                isCount++;
        }
        for(int i=0; i< str.length()  - 2; i++){
            if(str.charAt(i) == 'n' && str.charAt(i + 1) == 'o'  && str.charAt(i + 2) == 't')
                notCount++;
        }
        return isCount == notCount;
    }

//  Given a string, return the sum of the digits 0-9 that appear in the string, ignoring all other characters. Return
//  0 if there are no digits in the string. (Note: Character.isDigit(char) tests if a char is one of the chars
//  '0', '1', .. '9'. Integer.parseInt(string) converts a string to an int.)
    public int sumDigits(String str){
        int result = 0;
        for(int i =0; i<str.length(); i++){
            if(Character.isDigit(str.charAt(i)))
                result += Integer.parseInt(""+str.charAt(i));
        }
        return result;
    }



    public static void main(String[] args) {
        String3 obj = new String3();

        System.out.println(obj.sumNumbers("hoopla123asq"));
        System.out.println(obj.sumNumbers("aasas11basd33"));
        System.out.println(obj.sumNumbers("7 11"));
    }
}