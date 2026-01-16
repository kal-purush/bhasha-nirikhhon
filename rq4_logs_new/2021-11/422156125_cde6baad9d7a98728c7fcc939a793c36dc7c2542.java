package com.epam.tc.hw7jdi.entities;

import static java.lang.String.join;

import com.epam.jdi.tools.DataClass;
import java.util.ArrayList;
import java.util.List;

public class MetalsAndColorsData extends DataClass<MetalsAndColorsData> {
    private List<Integer> summary;
    private String color;
    private List<String> vegetables;
    private List<String> elements;
    private String metals;

    public List<Integer> getSummary() {
        return summary;
    }

    public String getColor() {
        return color;
    }

    public List<String> getVegetables() {
        return vegetables;
    }

    public List<String> getElements() {
        return elements;
    }

    public String getMetals() {
        return metals;
    }

    public List<String> getExpectedResultFromMACdata(MetalsAndColorsData data) {
        List<String> expectedResult = new ArrayList<>();

        expectedResult.add("Summary: " + (data.getSummary().get(0) + data.getSummary().get(1)));
        expectedResult.add("Elements: " + join(", ", data.getElements()));
        expectedResult.add("Color: " + data.getColor());
        expectedResult.add("Metal: " + data.getMetals());
        expectedResult.add("Vegetables: " + join(", ", data.getVegetables()));

        return expectedResult;
    }
}