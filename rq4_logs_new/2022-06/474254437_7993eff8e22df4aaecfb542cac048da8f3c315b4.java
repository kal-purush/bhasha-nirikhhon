package com.urise.webapp;

import com.urise.webapp.model.Resume;

public class MainHashCode {

    public static void main(String[] args) {
        Resume resume = ResumeTestData.doTest("name", "fullName");
        Resume resume1 = ResumeTestData.doTest("name", "fullName");

        System.out.println(resume.hashCode() + " = " + resume1.hashCode());
    }
}