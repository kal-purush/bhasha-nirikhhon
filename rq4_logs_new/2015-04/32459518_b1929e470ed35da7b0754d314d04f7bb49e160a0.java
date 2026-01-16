package com.freud.mrzz.frags;

import android.os.Bundle;
import android.support.v4.app.Fragment;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.widget.ImageView;

import com.freud.mrzz.R;

/**
 * Created by wy on 2015/3/21.
 */

public class colorTestFrag_testAty extends Fragment{
    ImageView[] colors,checks;

    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container, Bundle savedInstanceState) {
        View rootView=inflater.inflate(R.layout.fragment_color_testaty, container, false);
        colors=new ImageView[6];
        colors[0]=(ImageView) rootView.findViewById(R.id.iv_color1);
        colors[1]=(ImageView) rootView.findViewById(R.id.iv_color2);
        colors[2]=(ImageView) rootView.findViewById(R.id.iv_color3);
        colors[3]=(ImageView) rootView.findViewById(R.id.iv_color4);
        colors[4]=(ImageView) rootView.findViewById(R.id.iv_color5);
        colors[5]=(ImageView) rootView.findViewById(R.id.iv_color6);
        checks=new ImageView[6];
        checks[0]=(ImageView) rootView.findViewById(R.id.iv_color_check1);
        checks[1]=(ImageView) rootView.findViewById(R.id.iv_color_check2);
        checks[2]=(ImageView) rootView.findViewById(R.id.iv_color_check3);
        checks[3]=(ImageView) rootView.findViewById(R.id.iv_color_check4);
        checks[4]=(ImageView) rootView.findViewById(R.id.iv_color_check5);
        checks[5]=(ImageView) rootView.findViewById(R.id.iv_color_check6);
        for(int x=0;x<6;x++)
            colors[x].setOnClickListener(new View.OnClickListener() {
                @Override
                public void onClick(View v) {
                    int t=Integer.parseInt((String)v.getTag())-1;
                    for(int y=0;y<6;y++)
                    {
                        if(y==t)
                        {
                            checks[y].setVisibility(View.VISIBLE);
                        }else
                        {
                            checks[y].setVisibility(View.GONE);
                        }
                    }
                }
            });
        return rootView;
    }
}