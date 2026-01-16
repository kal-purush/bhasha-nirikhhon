package com.example.samplebatch.batch;

public class TestForListener {

    public String beforeOne(){
        String a = "hi, before";
        return a;
    }
    public String afterOne(){
        String a = "hi, after";
        return a;
    }
}