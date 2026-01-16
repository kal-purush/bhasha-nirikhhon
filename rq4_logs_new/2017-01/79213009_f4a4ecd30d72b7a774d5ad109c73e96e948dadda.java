package com.cruftlab.words.repository;

import com.cruftlab.words.model.Word;
import org.springframework.data.repository.CrudRepository;
import org.springframework.stereotype.Repository;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

@Repository
public interface WordRepository extends CrudRepository<Word, Long> {
    /**
     * Find random words
     * @param numberOfWords Fetch this many random words
     * @return Words found in the database
     */
    default Iterable<Word> findRandom(int numberOfWords) {
        final List<Word> allWords = new ArrayList<>();
        for (Word word : findAll()) {
            allWords.add(word);
        }
        final int wordCount = allWords.size();
        if (wordCount < numberOfWords) {
            throw new IllegalStateException("Required " + numberOfWords + " words, but found " + wordCount);
        }
        Collections.shuffle(allWords);
        return allWords.subList(0, numberOfWords);
    }
}