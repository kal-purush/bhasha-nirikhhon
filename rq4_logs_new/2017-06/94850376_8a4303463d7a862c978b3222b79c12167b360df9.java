package com.example.mitya.loftmoneytracker;

import android.os.Bundle;
import android.support.annotation.Nullable;
import android.support.v4.app.Fragment;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.LinearLayout;

/**
 * Created by Mitya on 26.06.2017.
 */

public class BalanceFragment extends Fragment {
    public static final String ARG_TYPE = "type";
    private String type;

    @Nullable
    @Override
    public View onCreateView(LayoutInflater inflater, @Nullable ViewGroup container, @Nullable Bundle savedInstanceState) {
        return inflater.inflate(R.layout.balance_fragment, null);
    }


    @Override
    public void onViewCreated(View view, @Nullable Bundle savedInstanceState) {
        super.onViewCreated(view, savedInstanceState);
        final LinearLayout layout = (LinearLayout) view.findViewById(R.id.balance);

//        final RecyclerView items = (RecyclerView) view.findViewById(R.id.items);
//        items.setAdapter(new ItemsAdapter());

        type = getArguments().getString(ARG_TYPE);


    }
}