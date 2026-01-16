package com.kodilla.testing.collection;

import org.junit.*;
import java.util.ArrayList;
import java.util.List;

public class CollectionTestSuite {

    @BeforeClass
    public static void beforeClass() {
        System.out.println("Collection Test Suite: begin");
    }
    @AfterClass
    public static void afterClass() {
        System.out.println("Collection Test Suite: end");
    }

    @Before
    public void before(){
        System.out.println("Test Case: begin");
    }

    @After
    public void after(){
        System.out.println("Test Case: end");
    }

    @Test
    public void testOddNumbersExterminatorEmptyList() {
        System.out.println("Testing empty list");
        //Given
        ArrayList<Integer> inputList = new ArrayList<>();
        OddNumbersExterminator oddNumbersExterminator = new OddNumbersExterminator();
        //When
        ArrayList<Integer> resultList = new ArrayList<>();
        resultList = oddNumbersExterminator.exterminate(inputList);
        for(Integer element: resultList) {
            System.out.println(element);
        }
        System.out.println("result list size: " + resultList.size());
        //Then
        Assert.assertEquals(resultList, inputList);
    }

    @Test
    public void testOddNumbersExterminatorNormalList() {
        System.out.println("Testing normal list");
        //Given
        ArrayList<Integer> inputList = new ArrayList<>();
        OddNumbersExterminator oddNumbersExterminator = new OddNumbersExterminator();
        for(int i = 0; i < 10; i++) {
            inputList.add(i+1);
        }
        //When
        ArrayList<Integer> resultList = oddNumbersExterminator.exterminate(inputList);
        System.out.println("result list size: " + resultList.size());
        ArrayList<Integer> resultPattern = new ArrayList<>();
        resultPattern.add(2);
        resultPattern.add(4);
        resultPattern.add(6);
        resultPattern.add(8);
        resultPattern.add(10);
        //Then
        Assert.assertEquals(resultList, resultPattern);
    }

}