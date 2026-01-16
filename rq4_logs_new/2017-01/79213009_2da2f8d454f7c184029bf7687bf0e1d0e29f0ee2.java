package com.cruftlab.words.service;

import com.cruftlab.words.model.CombinedWord;
import com.cruftlab.words.model.Word;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.test.context.junit4.SpringRunner;

import java.util.ArrayList;

@RunWith(SpringRunner.class)
@SpringBootTest
public class WordCombinatorServiceTest {
    @Mock
    private WordService wordService;

    private WordCombinatorService wordCombinatorService;

    @Before
    public void setupMocks() {
        Mockito.when(wordService.findRandom(2))
            .thenReturn(new ArrayList<Word>() {{
                add(new Word("gebiss"));
                add(new Word("sevje"));
            }});
        wordCombinatorService = new WordCombinatorService(wordService);
    }

    @Test
    public void assertWordsAreProperlyCombined() {
        final CombinedWord combinedWord = wordCombinatorService.combineRandom(2);
        Assert.assertEquals("gebissevje", combinedWord.getCombinedForm());
    }

    @After
    public void verifyMocks() {
        Mockito.verify(wordService);
    }
}