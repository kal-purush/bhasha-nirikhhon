package com.syntax.material_demo;

import android.app.Fragment;
import android.content.Intent;
import android.os.Bundle;
import android.support.annotation.Nullable;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.Toast;

/**
 * Created by Dumadu on 12/27/2016.
 */

public class IntroductionFragment extends Fragment implements View.OnClickListener{

    Button button_Introduction;
    @Nullable
    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container, Bundle savedInstanceState) {
        View view = inflater.inflate(R.layout.fragment_introduction, container, false);

        button_Introduction = (Button) view.findViewById(R.id.button_Introduction);
        button_Introduction.setOnClickListener(this);

        return view;
    }

    @Override
    public void onClick(View view) {

        switch (view.getId()){
            case R.id.button_Introduction :
                startActivity(new Intent(getActivity(), WelcomeActivity.class));
                break;
        }

    }
}