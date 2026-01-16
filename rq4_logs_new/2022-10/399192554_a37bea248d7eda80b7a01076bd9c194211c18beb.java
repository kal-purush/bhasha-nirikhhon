package com.github.ovorobeva.vocabularywordsservice.clients.apidocs;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

public interface TranslateApi {
    @RequestMapping(
            value = {"/translate/"},
            method = {RequestMethod.GET}
    )
    ResponseEntity<String> getTranslate(@RequestParam("text") String wordToTranslate,
                                        @RequestParam("auth_key") String apiKey,
                                        @RequestParam("source_lang") String sourceLanguage,
                                        @RequestParam("target_lang") String targetLanguage);
}