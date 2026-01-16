package com.kodilla.testing.collection;

import org.junit.*;
import java.util.*;

public class CollectionTestSuite {
    private List<Integer> testNumbers = new ArrayList<>();

    @Before
    public void start() {
        System.out.println("Rozpoczynam test.");
        testNumbers.add(23);
        testNumbers.add(10);
        testNumbers.add(4);
        testNumbers.add(15);
        testNumbers.add(78);
    }

    @After
    public void end() {
        System.out.println("Koniec testu.");
    }

    @Test
    public void testOddNumbersExterminatorEmptyList() {
        //Given
        OddNumbersExterminator oddNumbersExterminator = new OddNumbersExterminator();
        //When
        List<Integer> results = oddNumbersExterminator.exterminate(Collections.emptyList());
        System.out.println("Testing: " + results);
        //Then
        Assert.assertTrue(results.isEmpty());
    }

    @Test
    public void testOddNumbersExterminatorNormalList() {
        //Given
        OddNumbersExterminator oddNumbersExterminator = new OddNumbersExterminator();
        //When
        List<Integer> results = oddNumbersExterminator.exterminate(testNumbers);
        System.out.println("Testing: " + results);
        //Then
        Assert.assertEquals(results, Arrays.asList(10, 4, 78));
    }
}