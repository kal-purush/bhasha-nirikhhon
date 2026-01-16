package com.gerrardliu.test;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.FilenameUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.File;

public class HelloWorld {
    public String sayHello() {
        return "Hello Maven";
    }

    public static void testCommonsIO() {
        String rawFileFullName = "abc/123.txt/";
        String filefullName = FilenameUtils.normalizeNoEndSeparator(rawFileFullName,true);
        System.out.println(filefullName);
        try {
            FileUtils.writeStringToFile(new File(filefullName), "123", "UTF-8");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    public static void testCommonsLang3() {
        String rawString = "com.gerrardliu.test.hello";
        System.out.println(StringUtils.replaceChars(rawString,'.','/'));
    }

    public static void main(String[] args) {
        System.out.println(new HelloWorld().sayHello());
        //testCommonsIO();
        testCommonsLang3();
    }
}