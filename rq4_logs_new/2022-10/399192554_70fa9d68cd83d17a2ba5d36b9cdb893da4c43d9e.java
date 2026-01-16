
package com.github.ovorobeva.vocabularywordsservice.translates.testconf;

import com.github.ovorobeva.vocabularywordsservice.model.generated.GeneratedWordsDto;
import com.github.ovorobeva.vocabularywordsservice.model.translate.TranslateDto;
import com.github.ovorobeva.vocabularywordsservice.translates.Language;
import com.github.ovorobeva.vocabularywordsservice.translates.TranslateClient;
import org.springframework.http.ResponseEntity;

public class TestClientMock implements TranslateClient {

    @Override
    public void translateWord(GeneratedWordsDto word, Language language) {
        word.setCz(word.getEn() + "Cz");
        word.setFr(word.getEn() + "Fr");
        word.setRu(word.getEn() + "Ru");
    }

    @Override
    public ResponseEntity<TranslateDto> getTranslate(String wordToTranslate, String apiKey, String sourceLanguage, String targetLanguage) {
        return null;
    }
}