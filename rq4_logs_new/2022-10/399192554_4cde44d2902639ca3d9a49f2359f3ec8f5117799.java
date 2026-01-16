package com.github.ovorobeva.vocabularywordsservice.wordsprocessing.apidocs;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

public interface ProfanityCheckerApi {
    @RequestMapping(
            value = {"/containsprofanity/"},
            method = {RequestMethod.GET}
    )
    ResponseEntity<String> getLemma(@RequestParam("text") String word);
}