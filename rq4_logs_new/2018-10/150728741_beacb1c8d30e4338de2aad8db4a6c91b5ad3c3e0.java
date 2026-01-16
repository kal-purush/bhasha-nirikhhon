package com.daahae.damoyeo.view.activity;

import android.content.BroadcastReceiver;
import android.location.Location;
import android.os.Bundle;
import android.support.annotation.Nullable;
import android.support.v7.app.AppCompatActivity;
import android.util.Log;
import android.view.View;
import android.widget.ImageView;
import android.widget.TextView;

import com.arlib.floatingsearchview.FloatingSearchView;
import com.arlib.floatingsearchview.suggestions.SearchSuggestionsAdapter;
import com.arlib.floatingsearchview.suggestions.model.SearchSuggestion;
import com.daahae.damoyeo.R;
import com.daahae.damoyeo.view.data.ColorSuggestion;
import com.google.android.gms.common.api.PendingResult;

import java.util.ArrayList;
import java.util.List;

public class NMapSearchActivity extends AppCompatActivity {
    @Override
    protected void onCreate(@Nullable Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_nmap_search);

        final FloatingSearchView mSearchView = findViewById(R.id.floating_search_view);
        mSearchView.setOnQueryChangeListener(new FloatingSearchView.OnQueryChangeListener() {
            @Override
            public void onSearchTextChanged(String oldQuery, final String newQuery) {

                //get suggestions based on newQuery

                //pass them on to the search view
                List<ColorSuggestion> colorSuggestions = new ArrayList<ColorSuggestion>();
                colorSuggestions.add(new ColorSuggestion("purple"));
                colorSuggestions.add(new ColorSuggestion("green"));
                mSearchView.swapSuggestions(colorSuggestions);
            }
        });

        mSearchView.setOnBindSuggestionCallback(new SearchSuggestionsAdapter.OnBindSuggestionCallback() {
            @Override
            public void onBindSuggestion(View suggestionView, ImageView leftIcon, TextView textView, SearchSuggestion item, int itemPosition) {

                //here you can set some attributes for the suggestion's left icon and text. For example,
                //you can choose your favorite image-loading library for setting the left icon's image.
            }

        });
    }
}