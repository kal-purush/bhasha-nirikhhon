package InterviewCoding;

import java.security.GeneralSecurityException;
import java.util.ArrayList;

public class InterviewPractive {
    public static void main(String [] args){


        //sort array without using sort methods
        int [] arr={5,2,9,1,5,6};
        for(int i=0; i<arr.length-1 ; i++){
            for(int j =0; j<arr.length-i-1; j++){
                if(arr[j]>arr[j+1]){
                    int temp = arr[j];
                    arr[j]= arr[j+1];
                    arr[j+1] =temp;
                }
            }

        }
        for(int num:arr){
            System.out.print(num+ " ");
        }

        String sentence = "I love Java and selenium WebDriver";
        String[] words = sentence.split(" ");
        StringBuilder reversedSentence = new StringBuilder();
        for(int i = words.length-1; i>=0; i--){
            reversedSentence.append(words[i]).append(" ");
        }
        System.out.println(reversedSentence.toString().trim());


        try{
            int a = 10/0;
        }
        catch(ArithmeticException e){
            System.out.println("Arthimatic exception cought");
        }
        catch(NullPointerException n){
            System.out.println("Null pointer exception cought ");
        }
        catch (Exception e){
            System.out.println("General exception cuoght");
        }
        finally {
            System.out.println("finally block executed");
        }
        String s = "world";
        s.length(); // if s is not initialized , it threw compilation error like variable might have not been initialized .


        //reverse big words from the sentence
        String sentences = "reverse big words from the word";
        String[] word = sentences.split(" ");

        for(int i=0; i<word.length-1; i++){
            if(word[i].length() >3 ){
                word[i] = new StringBuilder(word[i]).reverse().toString();
                String result = String.join(" ", word);
                System.out.println(result);

            }
        }


    }
}