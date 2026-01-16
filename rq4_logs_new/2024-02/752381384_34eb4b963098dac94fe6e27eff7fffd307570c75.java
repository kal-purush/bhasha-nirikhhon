package com.example.repository;

import lombok.extern.slf4j.Slf4j;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.Random;
import java.util.concurrent.CopyOnWriteArrayList;

@Slf4j
public class PhraseStorage {
    private static final List<String> phrases = new CopyOnWriteArrayList<>();

    public static void addPhrase(String phrase) {
        phrases.add(phrase);
        log.info("New phrase added: {}", phrase);
    }

    public static String getRandomPhrase() throws NoSuchElementException {
        if (phrases.isEmpty()) {
            throw new NoSuchElementException();
        }
        String phrase = phrases.get(new Random().nextInt(phrases.size()));
        log.info("Output of the requested phrase: {}", phrase);
        return phrase;
    }

    public static void clearPhrases() {
        phrases.clear();
    }
}