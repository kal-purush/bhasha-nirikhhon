package com.cydeo.tests.day09_javafaker_driverUtil;

public class Singleton {

    //1-create private constructor
    private Singleton(){
    }

    //2-create private static String
    private static String word;

    //3-create utility method to return the "private string" we just created
    public static String getWord(){

       if (word == null){
           System.out.println("First time call. Word object is null");
           System.out.println("Assigning value to it now");
           word="something";
       }else{
           System.out.println("Word already has value");
       }

       return word;

    }

    public static void closeWord() {

        if (word != null) {
            /*
            This line will terminate the currently existing driver completely. It will not exist going forward.
             */

            word = null;

        }
    }


}