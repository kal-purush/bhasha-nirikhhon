package com.example.mariakovaleva.dortmundtourguide;


import android.os.Bundle;
import android.support.v4.app.Fragment;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ListView;

import java.util.ArrayList;
import java.util.Arrays;

/**
 * A simple {@link Fragment} subclass.
 */
public class BarsFragment extends Fragment {


    public BarsFragment() {

    }


    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container,
                             Bundle savedInstanceState) {
        View rootView = inflater.inflate(R.layout.fragment_list, container, false);

        ArrayList<Place> bars = new ArrayList<Place>(Arrays.asList(
                new Place(R.drawable.linie_403, R.string.bar_title_linie_403, R.string.bar_content_linie_403),
                new Place(R.drawable.marlene, R.string.bar_title_marlene, R.string.bar_content_marlene),
                new Place(R.drawable.domicil, R.string.bar_title_domicil, R.string.bar_content_domicil)));

        PlaceAdapter placeAdapter = new PlaceAdapter(getActivity(), bars);

        ListView listView = (ListView) rootView.findViewById(R.id.fragment_list);

        listView.setAdapter(placeAdapter);


        return rootView;
    }

}