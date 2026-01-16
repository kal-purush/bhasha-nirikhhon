package controller;

import java.util.Scanner;
import java.util.stream.Collectors;
import java.util.*;

import model.CustomStemmer;
import model.Location;
import view.MainMenu;

public class SearchController {

    private static SearchController instance;

    static {
        instance = new SearchController();
    }
    
    private SearchController() {
    }

    public static SearchController getInstance() {
        return instance;
    }

    public void run() {
        String command;
        Scanner scanner = ProgramController.getScanner();
        System.out.println("Please enter word for search ($back for back to main menu): ");
        while(true) {
            command = scanner.nextLine();
            if (command.equals("$back")) MainMenu.run();
            else search(command);
        }
    }

    public HashSet<String> search(String searchFor) {
        String[] words = searchFor.split("[\\s]+");
        ArrayList<HashSet<String>> noTags = new ArrayList<>();
        ArrayList<HashSet<String>> plusTags = new ArrayList<>();
        ArrayList<HashSet<String>> minusTags = new ArrayList<>();
        boolean isPositive = false;
        for (String word : words) {
            if (word.startsWith("-")) addToList(word.substring(1), minusTags);
            else if (word.startsWith("+")) { 
                addToList(word.substring(1), plusTags);
                isPositive = true;
            } else addToList(word, noTags);
        }
        System.out.println("noTags: " + noTags.size() + "\n" + "plusTags: " + plusTags.size() + "\n" + "minusTags: " + minusTags.size());
        // System.out.println(noTags.get(0));
        HashSet<String> pluses = new HashSet();
        for (HashSet<String> plusTag : plusTags) pluses.addAll(plusTag);
        HashSet<String> minuses = new HashSet();
        for (HashSet<String> minusTag : minusTags) minuses.addAll(minusTag);
        HashSet<String> answers = noTags.size() == 0 ? new HashSet() : noTags.get(0);
        for (String string : answers) {
            for (HashSet<String> noTag : noTags) {
                if (!noTag.contains(string)) {
                    answers.remove(string); 
                    break;
                }
            }
        }
        final boolean isPositiveFinal = isPositive;
        answers.removeIf(e -> {
            if (minusTags.contains(e)) return true;
            if (isPositiveFinal && !plusTags.contains(e)) return true;
            return false;
        });
        printResult(answers);
        return answers; 
    }

    private void addToList(String word, ArrayList<HashSet<String>> tags) {
        CustomStemmer wordStemmer = new CustomStemmer(word);
        HashSet<String> hashSet = new HashSet();
        HashMap<String, ArrayList<Location>> datas = MainMenuController.getDatas();
        tags.add(hashSet);
        if (!datas.containsKey(wordStemmer.toString())) return;
        ArrayList<Location> locations = datas.get(wordStemmer.toString());
        for (Location location : locations) hashSet.add(location.getFileName());
    }

    private void printResult(HashSet<String> results) {
        System.out.println(results);
    }
}