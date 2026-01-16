package edu.eitc;

public class BadStar extends Star{
    String getColor(){
        if (poked){
            return "Red";
        }else{
            return "White";
        }
    }
}