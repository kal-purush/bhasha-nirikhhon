package com.example.minorius.pr_8;

import android.content.Context;
import android.view.View;
import android.widget.Toast;

import java.util.ArrayList;
import java.util.Random;

/**
 * Created by Minorius on 22.02.2016.
 */
public class Subjects {

    Category_activity ca;
    float alfa = 1.0f;

    public void getCheck(int n, Context c){

        for(int i = 0; i < ca.test.size(); i++){
            if(i == n && Category_activity.test.get(i).isChecked()){
                ca.al.add(ca.test2.get(i));
                ca.al.get(ca.al.size()-1).setVisibility(View.VISIBLE);
                ca.footer_2.setVisibility(View.VISIBLE);
                break;
            }
            else if(i == n && !(Category_activity.test.get(i).isChecked())){
                ca.al.get(ca.al.indexOf(ca.test2.get(i))).setVisibility(View.GONE);
                ca.al.remove(ca.al.indexOf(ca.test2.get(i)));
            }
        }

        if(ca.al.size() == 0){
            ca.footer_2.setVisibility(View.INVISIBLE);
        }

        for(int i = ca.al.size(); i > 0; i--){
            ca.al.get(i-1).setAlpha(alfa - 0.1f);
            this.alfa = alfa - 0.1f;
        }

        this.alfa = 1.0f;

    }
}