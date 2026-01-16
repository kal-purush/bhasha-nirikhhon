package com.concon.talkabout.talkabout;

import android.os.Bundle;
import android.support.v4.app.ListFragment;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.TextView;

/**
 * Created by Pablitoh on 02/06/2015.
 */
public class ServerRules extends ListFragment {

    public static ServerRules newInstance(int type) {
        Bundle args = new Bundle();
        args.putInt("TYPE",type);
        ServerRules fragment = new ServerRules();
        fragment.setArguments(args);
        return fragment;
    }
    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container, Bundle savedInstanceState) {
        View view = inflater.inflate(R.layout.server_rules, container, false);
        TextView sectionTitle = (TextView) view.findViewById(R.id.myrulestitle);


        switch (getArguments().getInt("TYPE"))
        {
            case 0:
            sectionTitle.setText("Popular Today");
                break;
            case 1:sectionTitle.setText("Recent");
                break;
            default:
                throw new RuntimeException("No valid type");
        }

        return view;
    }
}