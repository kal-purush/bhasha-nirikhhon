package com.example.ivan.gitbrowser;

import android.content.SearchRecentSuggestionsProvider;

/**
 * Created by ivan on 05.04.18.
 */

public class GbSuggestionProvider extends SearchRecentSuggestionsProvider {
    public final static String AUTHORITY = "com.example.ivan.gitbrowser.GbSuggestionProvider";
    public final static int MODE = DATABASE_MODE_QUERIES;

    public GbSuggestionProvider() {
        setupSuggestions(AUTHORITY, MODE);
    }
}