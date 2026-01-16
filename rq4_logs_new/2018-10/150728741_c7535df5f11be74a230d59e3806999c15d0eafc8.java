package com.daahae.damoyeo.presenter;

import android.os.Bundle;
import android.support.v4.app.FragmentManager;
import android.support.v4.app.FragmentTransaction;

import com.daahae.damoyeo.R;
import com.daahae.damoyeo.view.activity.NMapActivity;
import com.daahae.damoyeo.view.activity.NMapFragment;

public class NMapActivityPresenter {
    private NMapActivity view;// 뷰
    //모델은 각자 클래스 생성

    public NMapActivityPresenter(NMapActivity view){
        this.view = view;
        NMapFragment NMapFragment = new NMapFragment();
        NMapFragment.setArguments(new Bundle());
        FragmentManager fm = this.view.getSupportFragmentManager();
        FragmentTransaction fragmentTransaction = fm.beginTransaction();
        fragmentTransaction.add(R.id.fragmentHere, NMapFragment);
        fragmentTransaction.commit();
    }
}