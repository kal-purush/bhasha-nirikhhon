package com.tsystems;
import java.util.ArrayList;
import java.util.Scanner;

public class Quiz {
    /*public static ArrayList QuizConstructor(ArrayList<String> rawQuestion){

    }*/
    public static ArrayList QuestionTest(){
        ArrayList<String> rawQuestionRow = new ArrayList<>();
        rawQuestionRow.add("Hány éves a norvég király?");
        rawQuestionRow.add("12");
        rawQuestionRow.add("34");
        rawQuestionRow.add("45");
        rawQuestionRow.add("33");
        rawQuestionRow.add("33");
        return rawQuestionRow;
    }
    public static void QA(ArrayList<String>Question){
        AskQuestion(Question);
        GetAnswer(Question);
    }
    public static void AskQuestion(ArrayList<String>Question){
        for (int i = 0; i < Question.size()-1; i++) {
            if (i > 0) {
                System.out.println((i) + ". " + Question.get(i));
            } else {
                System.out.println(Question.get(i));
            }
        }
    }
    public static void GetAnswer(ArrayList<String>Question){
        Scanner in = new Scanner(System.in);
        int rightOne = Question.indexOf(Question.get(5));
        int userAnswer = in.nextInt();

        System.out.println("userAnswer is " + userAnswer+ "." + "The right answer is " + rightOne + ".");
        if (rightOne == userAnswer) {
            System.out.println("Match!");
        } else {
            System.out.println("Nope.");
        }
    }
}