package com.example.appchiasecongthucnauan.utils;

import com.example.appchiasecongthucnauan.models.search.SearchResultDto;

public class SearchState {
    private static SearchState instance;

    private SearchResultDto lastSearchResults;
    private SearchState() {}
    public static SearchState getInstance() {
        if (instance == null) {
            instance = new SearchState();
        }
        return instance;
    }

    public void setSearchResults(SearchResultDto results) {
        this.lastSearchResults = results;
    }
    public SearchResultDto getLastSearchResults() {
        return lastSearchResults;
    }
    public void clear() {
        lastSearchResults = null;
    }
}