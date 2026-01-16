package utils;

import java.util.Random;

public class SentenceGenerator {

    private static final String[] subjects = {
            "The cat", "A dog", "The teacher", "My friend", "The programmer"
    };

    private static final String[] verbs = {
            "runs", "jumps", "writes", "plays", "sings"
    };

    private static final String[] objects = {
            "a book", "in the park", "some code", "a song", "football"
    };

    private static final String[] adverbs = {
            "quickly", "loudly", "happily", "carefully", "slowly"
    };

    public static String generateSentence() {
        Random random = new Random();
        String subject = subjects[random.nextInt(subjects.length)];
        String verb = verbs[random.nextInt(verbs.length)];
        String object = objects[random.nextInt(objects.length)];
        String adverb = adverbs[random.nextInt(adverbs.length)];

        return subject + " " + verb + " " + object + " " + adverb;
    }
}