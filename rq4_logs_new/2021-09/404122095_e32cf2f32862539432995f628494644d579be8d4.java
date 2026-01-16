package com.engine;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

public class Parser {

    public static String[] parseInput(String command) {
        String returnValue[] = new String[2];
        String[] explodedCommand = command.split("\\s+");
        if (explodedCommand.length == 2) {
            returnValue = explodedCommand;
        }

        //TODO: run explodedCommand[0] through synonymAnalyzer first
        returnValue[0] = explodedCommand[0];

        if (explodedCommand.length > 1) {
            System.out.println("Removing some words, maybe");
            returnValue[1] = removeExtraWords(Arrays.copyOfRange(explodedCommand, 1, explodedCommand.length));
        }

        return returnValue;
    }

    private static String removeExtraWords(String[] commandStub) {
        String[] noOpWords = {
                "the",
                "this",
                "that",
                "those",
                "it",
                "she",
                "he",
                "they",
                "hers",
                "her",
                "his",
                "him",
                "them",
                "their",
                "I",
                "me",
                "my",
                "we",
                "us",
                "our",
                "a",
                "an",
                "about",
                "above",
                "across",
                "after",
                "against",
                "along",
                "as",
                "aside",
                "at",
                "away",
                "before",
                "behind",
                "below",
                "beneath",
                "beside",
                "besides",
                "between",
                "beyond",
                "down",
                "during",
                "in",
                "into",
                "inside",
                "for",
                "from",
                "like",
                "near",
                "next",
                "next",
                "past",
                "off",
                "on",
                "onto",
                "out",
                "of",
                "over",
                "through",
                "throughout",
                "to",
                "toward",
                "towards",
                "under",
                "underneath",
                "until",
                "unto",
                "up",
                "upon",
                "with",
                "within",
                "without"
        };
        List<String> noOpList = new ArrayList<>(Arrays.asList(noOpWords));
        List<String> stub = new ArrayList<>(Arrays.asList(commandStub));
        for (String i : noOpList) {
            if (stub.contains(i)) {
                stub.remove(stub.indexOf(i));
            }
        }

        return String.join(" ", stub);
    }
}