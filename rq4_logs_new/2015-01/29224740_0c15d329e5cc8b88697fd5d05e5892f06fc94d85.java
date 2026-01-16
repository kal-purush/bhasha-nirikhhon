package com.juangdiaz.bookshelf.fragments;

import android.app.Activity;
import android.net.Uri;
import android.os.Bundle;
import android.app.Fragment;
import android.support.v7.app.ActionBarActivity;
import android.text.Html;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.TextView;

import com.juangdiaz.bookshelf.R;
import com.juangdiaz.bookshelf.model.Book;


import com.google.common.base.Strings;

import butterknife.ButterKnife;
import butterknife.InjectView;


public class BookDetailFragment extends Fragment {


    public static final String ARG_ITEM = "selected_item";

    private static final String SAVED_LAST_TITLE = "last_title";

    private Book mBook; // the selected item

    @InjectView(R.id.item_detail_title)
    TextView bookDetailTitle;


    public BookDetailFragment() {
        // Required empty public constructor
    }

    @Override
    public void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        if (getArguments().containsKey(ARG_ITEM)) {
            mBook = getArguments().getParcelable(ARG_ITEM); // get item from bundle
        }
    }

    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container,
                             Bundle savedInstanceState) {
        View rootView = inflater.inflate(R.layout.fragment_book_detail, container, false);
        ButterKnife.inject(this, rootView);

        // Show the content.
        if (mBook != null) {
        //TODO:add book details
            if (!Strings.isNullOrEmpty(mBook.getTitle())) {
                ((ActionBarActivity) getActivity()).getSupportActionBar().setTitle(Html.fromHtml(mBook.getTitle()).toString()); // title in the action bar
                bookDetailTitle.setText(Html.fromHtml(mBook.getTitle()).toString());
            }
        }

        return rootView;
    }


    @Override
    public void onViewCreated(View view, Bundle savedInstanceState) {
        super.onViewCreated(view, savedInstanceState);

        // Restore the previously serialized activated item position.
        if (savedInstanceState != null) {
            if (savedInstanceState.containsKey(SAVED_LAST_TITLE)) {
                ((ActionBarActivity) getActivity()).getSupportActionBar().setTitle(savedInstanceState.getString(SAVED_LAST_TITLE));
            }
        }
    }

    @Override
    public void onSaveInstanceState(Bundle outState) {
        super.onSaveInstanceState(outState);
        CharSequence actionBarTitle = ((ActionBarActivity) getActivity()).getSupportActionBar().getTitle();
        if (actionBarTitle != null && actionBarTitle.length() > 0) {
            outState.putString(SAVED_LAST_TITLE, actionBarTitle.toString());
        }
    }

}