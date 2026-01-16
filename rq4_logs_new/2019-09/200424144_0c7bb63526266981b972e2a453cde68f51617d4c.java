package com.kodilla.testing.collection;

import com.kodilla.testing.collection.OddNumbersExterminator;

import org.junit.*;
import java.util.*;


public class CollectionTestSuite {
    public OddNumbersExterminator oddNumbersExterminator = new OddNumbersExterminator();

    @Before
    public  void before() {
        System.out.println("Test case: begin");
    }

    @After
    public void after() {
        System.out.println("Test case: end");
    }

    @BeforeClass
    public void beforeClass() {
        System.out.println("Test suite: begin");
    }

    @AfterClass
    public void afterClass() {
        System.out.println("Test suite: end");
    }

    @Test
    public void testOddNumbersExterminatorEmptyList() {
        //Given
        ArrayList<Integer> emptyList = new ArrayList<Integer>();
        //When
        ArrayList resultList = new ArrayList<>(oddNumbersExterminator.exterminate(emptyList));
        //Then
        Assert.assertEquals(emptyList, resultList);
    }
    @Test
    public void testOddNumbersExterminatorNormalList() {
        //Given
        ArrayList<Integer> testNumbers = new ArrayList<Integer>();
        for(int i = 1; i <= 10; i++) {
            testNumbers.add(i);
        }
        //When
        ArrayList<Integer> resultEvenNumbers = new ArrayList<>(oddNumbersExterminator.exterminate(testNumbers));
        //Then
        ArrayList<Integer> expectedList = new ArrayList<Integer>();
        for(int n = 2; n <= 10; n =+ 2) {
            expectedList.add(n);
        }
        Assert.assertEquals(expectedList, resultEvenNumbers);
    }
}