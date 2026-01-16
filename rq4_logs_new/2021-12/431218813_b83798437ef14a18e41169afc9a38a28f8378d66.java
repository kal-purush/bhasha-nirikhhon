package com.company;

import java.util.ArrayList;


public class Main {

    public static void main(String[] args) {
        ArrayList<String> hats = new ArrayList<>();
            hats.add("blank");
            hats.add("snap-backs");
            hats.add("fitted");
            hats.add("cowboy");

//            hats.removeAll(hats);
//        for(int i = 0; i <hats.size(); i++){
//            System.out.println(hats.get(i));
//        }
//        for(String i : hats){
        for(int i = 0; i < hats.size(); i++){
           if(hats.get(i) == "snap-backs"){
               hats.remove(i);
           }
        }
        System.out.println(hats);

//   int x = 7;
//    if(x < 10){
//        System.out.println("less then 10");
//    }
    }
}