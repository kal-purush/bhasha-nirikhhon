package com.wiki.search.controllers;

import com.wiki.search.models.WikiArticleModel;
import com.wiki.search.services.LuceneSearchService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

@RestController
class SearchController {

    private final Logger log = LogManager.getLogger(this.getClass());
    private final LuceneSearchService searchService;

    SearchController(LuceneSearchService searchService) {
        this.searchService = searchService;
    }

    @GetMapping("/search")
    @CrossOrigin(origins = "http://localhost:4200")
    public ResponseEntity<List<WikiArticleModel>> getWikiArticleDetails(
            @RequestParam(value="term", required = true) String searchTerm,
            @RequestParam(value="size", required = true) String searchSize) {

        List<WikiArticleModel> wikiArticleModelList = searchService.search(searchTerm, searchSize);

        if(wikiArticleModelList!= null) {
            return new ResponseEntity<>(wikiArticleModelList, HttpStatus.OK);
        } else {
            log.error("Failed to retrieve wiki search list");
            return new ResponseEntity<>(HttpStatus.NOT_FOUND);
        }
    }
}