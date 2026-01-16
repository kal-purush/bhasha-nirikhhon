package com.francony.romain.channelmessaging.fragments;


import android.content.Intent;
import android.content.SharedPreferences;
import android.os.Bundle;
import android.support.design.widget.FloatingActionButton;
import android.support.v4.app.Fragment;
import android.support.v7.widget.Toolbar;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ListView;

import com.francony.romain.channelmessaging.Channel;
import com.francony.romain.channelmessaging.Channels;
import com.francony.romain.channelmessaging.Connexion;
import com.francony.romain.channelmessaging.Friend;
import com.francony.romain.channelmessaging.ListeArrayAdapter;
import com.francony.romain.channelmessaging.LoginActivity;
import com.francony.romain.channelmessaging.OnDowloadCompleteListener;
import com.francony.romain.channelmessaging.R;
import com.google.gson.Gson;

import java.util.HashMap;
import java.util.Timer;
import java.util.TimerTask;

/**
 * A simple {@link Fragment} subclass.
 */
public class ChannelListFragment extends Fragment {
    public String getToken() {
        return token;
    }

    public void setToken(String token) {
        this.token = token;
    }

    public String token;
    public Connexion connexion;

    public FloatingActionButton getFab() {
        return fab;
    }

    public void setFab(FloatingActionButton fab) {
        this.fab = fab;
    }

    public Connexion getConnexion() {
        return connexion;
    }

    public void setConnexion(Connexion connexion) {
        this.connexion = connexion;
    }

    public ListView getListView() {
        return listView;
    }

    public void setListView(ListView listView) {
        this.listView = listView;
    }

    public ListView listView;
    public FloatingActionButton fab;


    public ChannelListFragment() {
        // Required empty public constructor
    }


    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container, Bundle savedInstanceState) {
        // Inflate the layout for this fragment
        View v = inflater.inflate(R.layout.fragment_channel_list, container, false);
        fab =(FloatingActionButton) v.findViewById(R.id.fab);

        listView = (ListView) v.findViewById(R.id.channels);
        fab.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent intent = new Intent(getActivity().getApplicationContext(),Friend.class);
                startActivity(intent);
            }
        });


        listView.setOnItemClickListener((Channel)getActivity());



        connexion = new Connexion("http://www.raphaelbischof.fr/messaging/?function=getchannels");
        HashMap<String,String> params = new HashMap<>();
        SharedPreferences settings = getActivity().getSharedPreferences(LoginActivity.STOCKAGE, 0);
        params.put("accesstoken", settings.getString("token", "hello"));
        connexion.setParmetres(params);
        connexion.setOnNewsUpdateListener((Channel)getActivity());
        connexion.execute();
        return v;

    }

    @Override
    public void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);








    }



}