package com.github.ovorobeva.vocabularywordsservice.wordsprocessing.apidocs;

import com.github.ovorobeva.vocabularywordsservice.model.partsofspeech.PartsOfSpeechDto;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;

import java.util.List;

public interface PartsOfSpeechApi {
    @RequestMapping(
            value = {"/en/{word}"},
            method = {RequestMethod.GET}
    )
    ResponseEntity<List<PartsOfSpeechDto>> findPartsOfSpeech(@PathVariable("word") String word);
}