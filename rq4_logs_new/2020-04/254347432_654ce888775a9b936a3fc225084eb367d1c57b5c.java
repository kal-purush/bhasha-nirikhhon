package com.tgt.ignite;

import java.util.HashSet;
import java.util.Set;

public class HashSetApp {

    public static void main(String[] args) {

        Set<String> skill = new HashSet<>();
        skill.add("Java");
        skill.add("SQL");
        skill.add("NoSQL");
        skill.add("Java");

        for (String str: skill){
            System.out.println(str);
        }


    }
}