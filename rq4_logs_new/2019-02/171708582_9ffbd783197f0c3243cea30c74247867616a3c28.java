package com.example.treblehelper;

import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class CheckStudentGetAndSet {
    @Test
    public void canGetAndSetStudentInfo() {
        Student s = new Student("James", "Evans", "09/12/2000", 8005000,
                "example@byui.edu", "Flute",
                "JamesEvans", "Jam3sIsTh3b3st");
        System.out.println(s.getFirstName() == "James");
        System.out.println(s.getLastName() == "Evans");
        System.out.println(s.getBirthday() == "09/12/2000");
        System.out.println(s.getPhoneNumber() == 8005000);
        System.out.println(s.getEmail() == "example@byui.edu");
        System.out.println(s.getInstrument() == "Flute");
        System.out.println(s.getUsername() == "JamesEvans");
        System.out.println(s.getPassword() == "Jam3sIsTh3b3st");
    }

}