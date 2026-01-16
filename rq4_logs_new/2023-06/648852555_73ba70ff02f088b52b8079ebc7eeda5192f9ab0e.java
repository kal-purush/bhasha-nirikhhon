package com.cydeo.tests.day08_properties_config_reader;

import org.testng.annotations.Test;

public class LearningProperties {

    //Java is tracking some information about our computer and our project, and username etc...

    @Test
    public void java_properties_reading_test(){

        //key = value
        //we pass key, it will return value
        System.out.println("System.getProperty(\"os.name\") = " + System.getProperty("os.name"));
        System.out.println("System.getProperty(\"user.name\") = " + System.getProperty("user.name"));

    }
}