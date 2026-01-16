package com.example.demo;

import org.springframework.stereotype.Component;

//@Component
public class CanonPrinter implements Printer{

    @Override
    public void print(String msg) {
        System.out.println("canon印表機:"+msg);
    }
}