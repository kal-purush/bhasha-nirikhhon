package com.example.myapplication.activity;

import androidx.appcompat.app.AppCompatActivity;

import android.os.Bundle;
import android.util.Log;
import android.view.View;
import android.widget.Button;

import com.example.myapplication.R;
import com.example.myapplication.algorithm.RecursionTest;
import com.example.myapplication.databinding.ActivityTestSheJiBinding;
import com.example.myapplication.shejimoshi.FactoryTest;

public class TestSheJiActivity extends AppCompatActivity implements View.OnClickListener {
    private ActivityTestSheJiBinding binding;
    private Button btn_factory,btn_digui;
    private int clickId;
    private static final String TAG = TestSheJiActivity.class.toString();

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        binding = ActivityTestSheJiBinding.inflate(getLayoutInflater());
        setContentView(binding.getRoot());
        initView();
        initAction();
    }

    private void initAction() {
        btn_factory.setOnClickListener(this::onClick);
        btn_digui.setOnClickListener(this::onClick);
    }

    private void initView() {
        btn_factory = binding.btnFactory;
        btn_digui = binding.btnDigui;
    }

    @Override
    public void onClick(View v) {
        clickId = v.getId();
        if(clickId==binding.btnFactory.getId()){
            FactoryTest test = new FactoryTest();
            test.show(new FactoryTest.Fox(),new FactoryTest.FoxFarm());
        }else if(clickId==binding.btnDigui.getId()){
            RecursionTest recursionTest = new RecursionTest();
            int[][] ints = recursionTest.setMap();
            for (int i=0;i<ints.length;i++){
                for (int j = 0; j<ints[0].length;j++){
                    Log.d(TAG, "onClick: "+ints[i][j]);
                }
            }
        }
    }
}