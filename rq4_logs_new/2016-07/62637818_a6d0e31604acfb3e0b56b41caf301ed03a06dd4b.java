package com.madebykamil;


import com.madebykamil.jms.MessageSender;
import org.springframework.context.ApplicationContext;
import org.springframework.context.support.ClassPathXmlApplicationContext;

import java.io.Console;
import java.util.HashMap;
import java.util.Map;

public class Main {

    public static void main(String[] args) {
        ApplicationContext applicationContext = new ClassPathXmlApplicationContext("/application-context.xml");

        MessageSender sender = (MessageSender) applicationContext.getBean("messageSender");
        sender.send("2,5,+");
    }

}