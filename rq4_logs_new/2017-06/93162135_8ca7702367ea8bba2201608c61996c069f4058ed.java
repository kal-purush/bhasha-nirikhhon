package com.example.shenjack.zhihudailyreader.topstories;

import android.os.Bundle;
import android.support.annotation.Nullable;
import android.support.v4.app.Fragment;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;

/**
 * Created by ShenJack on 2017/6/21.
 */

public class NewFragment extends Fragment {
    private View localView;

    @Override
    public void onCreate(@Nullable Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
    }



    public NewFragment() {
    }


    public static NewFragment newInstance(View view) {

        Bundle args = new Bundle();

        NewFragment fragment = new NewFragment();
        fragment.setArguments(args);

        fragment.setView(view);

        return fragment;
    }

    @Nullable
    @Override
    public View onCreateView(LayoutInflater inflater, @Nullable ViewGroup container, Bundle savedInstanceState) {
        return localView;
    }

    public void setView(View view) {
        this.localView = view;
    }
}