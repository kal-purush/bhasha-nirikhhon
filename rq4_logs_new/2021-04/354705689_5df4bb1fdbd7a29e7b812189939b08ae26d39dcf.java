package com.company;

import org.junit.*;
import org.junit.jupiter.api.BeforeAll;

import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class TestLogic {
    public static Main obj = new Main();
    @BeforeAll
    public static void init(){
        obj.loadSampleData();
    }
    @BeforeClass
    public static void setUpBeforeClass() throws Exception {
        System.out.println("Before all");
    }
    @Before
    public void setUpBeforeMethod() throws Exception {
        System.out.println("Before each method");
    }
    @AfterClass
    public static void setUpAfterClass() throws Exception {
        System.out.println("After class");
    }
    @After
    public void setUpAfterMethod() throws Exception {
        System.out.println("After each method");
    }
    @Test
    public void testCheckStudentId(){
        System.out.println("Test checkstudentId");
        assertTrue(obj.checkStudentId("s3742891"));
        assertFalse(obj.checkStudentId("s3742981"));
    }

    @Test
    public void testCheckCourseId(){
        System.out.println("Check course id");
        assertTrue(obj.checkCourseId("cosc123"));
        assertFalse(obj.checkCourseId("cosc126"));
    }

    @Test
    public void testCheckStudentEnrollment(){
        System.out.println("Check student enrollment.\n" +
                "Result of this test might change in case of further update to enrollment list.");
        assertTrue(obj.checkStudentEnrollment("s3742891","2021A"));
        assertFalse(obj.checkStudentEnrollment("s3742891","2022A"));
    }

    @Test
    public void testCheckCourseEnrollment(){
        System.out.println("Check course enrollment.\n" +
                "Result of this test might change in case of further update to enrollment list.");
        assertTrue(obj.checkStudentEnrollment("cosc123","2021A"));
        assertFalse(obj.checkStudentEnrollment("cosc126","2022A"));
    }

    @Test
    public void testAdd(){
        System.out.println("Test add enrollment");
        obj.add("s3753240", "cosc124","2021A");
        assertTrue(obj.checkStudentEnrollment("s3753240","2021A"));
    }

    @Test
    public void testDelete() throws InterruptedException {
        System.out.println("Tets delete enrollment");
        obj.delete("s3742891","cosc123","2021A");
        assertFalse(obj.checkCourseEnrollment("cosc123","2021A"));
    }
}