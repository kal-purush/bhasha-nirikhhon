package com.sun.music_64.screen.home;

import android.os.Bundle;
import android.support.annotation.NonNull;
import android.support.annotation.Nullable;
import android.support.v4.app.Fragment;
import android.support.v7.app.AppCompatActivity;
import android.support.v7.widget.Toolbar;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;
import android.widget.TextView;

import com.sun.music_64.R;

public class HomeFragment extends Fragment {
    public static HomeFragment sInstance;
    private Toolbar mToolbar;
    private View mSearchView;
    private TextView mTextGenres;
    private TextView mTextSuggestedSong;
    private ImageView mImageAllMusic;
    private ImageView mImageAllAudio;
    private ImageView mImageAmbient;
    private ImageView mImageClassic;
    private ImageView mImageCountry;
    private ImageView mImageRock;

    public static HomeFragment getInstance() {
        if (sInstance == null) {
            sInstance = new HomeFragment();
        }
        return sInstance;
    }

    @Nullable
    @Override
    public View onCreateView(@NonNull LayoutInflater inflater,
                             @Nullable ViewGroup container,
                             @Nullable Bundle savedInstanceState) {
        View view = inflater.inflate(R.layout.fragment_home, container, false);
        initUi(view);
        return view;
    }

    private void initUi(View view) {
        mToolbar = view.findViewById(R.id.toolbar);
        ((AppCompatActivity) getActivity()).setSupportActionBar(mToolbar);
        mTextGenres = view.findViewById(R.id.text_genre);
        mSearchView = view.findViewById(R.id.search);
        mTextSuggestedSong = view.findViewById(R.id.text_suggested_song);
        mImageAllMusic = view.findViewById(R.id.image_all_music);
        mImageAllAudio = view.findViewById(R.id.image_all_audio);
        mImageAmbient = view.findViewById(R.id.image_ambient);
        mImageClassic = view.findViewById(R.id.image_classical);
        mImageCountry = view.findViewById(R.id.image_country);
        mImageRock = view.findViewById(R.id.image_rock);
    }

}