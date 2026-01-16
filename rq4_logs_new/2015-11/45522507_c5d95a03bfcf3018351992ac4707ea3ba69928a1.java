package com.example.tesla.u_smart;

import android.app.Fragment;
import android.os.Bundle;
import android.support.annotation.Nullable;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;

public class Hicheeliin_huwaari extends Fragment {
    private View inflateV;

    @Nullable
    @Override
    public View onCreateView(LayoutInflater inflater, ViewGroup container, Bundle savedInstanceState) {
         inflateV =  inflater.inflate(R.layout.activity_hicheeliin_huwaari,null);
        return inflateV;
    }
   

}