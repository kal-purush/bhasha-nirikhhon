package com.smapley.powerwork.activity;

import android.content.Intent;
import android.support.v4.app.FragmentManager;
import android.support.v4.app.FragmentTransaction;
import android.support.v7.widget.RecyclerView;
import android.view.View;
import android.widget.FrameLayout;
import android.widget.TextView;

import com.lidroid.xutils.view.annotation.ContentView;
import com.lidroid.xutils.view.annotation.ViewInject;
import com.lidroid.xutils.view.annotation.event.OnClick;
import com.smapley.powerwork.R;
import com.smapley.powerwork.fragment.BaseFragment;
import com.smapley.powerwork.fragment.Pro_Item1;
import com.smapley.powerwork.fragment.Pro_Item2;
import com.smapley.powerwork.fragment.Pro_Item3;
import com.smapley.powerwork.fragment.Pro_Item4;
import com.smapley.powerwork.fragment.Pro_Item5;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by smapley on 15/11/16.
 */
@ContentView(R.layout.activity_project)
public class Project extends BaseActivity {


    @ViewInject(R.id.pro_fl_content)
    private FrameLayout pro_fl_content;

    @ViewInject(R.id.pro_tv_btn_item1)
    private TextView pro_tv_btn_item1;
    @ViewInject(R.id.pro_tv_btn_item2)
    private TextView pro_tv_btn_item2;
    @ViewInject(R.id.pro_tv_btn_item3)
    private TextView pro_tv_btn_item3;
    @ViewInject(R.id.pro_tv_btn_item4)
    private TextView pro_tv_btn_item4;
    @ViewInject(R.id.pro_tv_btn_item5)
    private TextView pro_tv_btn_item5;

    private Pro_Item1 pro_item1;
    private Pro_Item2 pro_item2;
    private Pro_Item3 pro_item3;
    private Pro_Item4 pro_item4;
    private Pro_Item5 pro_item5;

    private List<BaseFragment> pro_lt_fragment;

    private FragmentManager fragmentManager;


    private String pro_name;
    private int pro_id;

    @Override
    protected void initParams() {
        initData();
        initView();
        initFragment();
    }

    private void initView() {
    }

    private void initFragment() {
        pro_lt_fragment = new ArrayList<>();
        pro_item1 = new Pro_Item1();
        pro_item2 = new Pro_Item2();
        pro_item3 = new Pro_Item3();
        pro_item4 = new Pro_Item4();
        pro_item5 = new Pro_Item5();
        pro_lt_fragment.add(pro_item1);
        pro_lt_fragment.add(pro_item2);
        pro_lt_fragment.add(pro_item3);
        pro_lt_fragment.add(pro_item4);
        pro_lt_fragment.add(pro_item5);
        transactionTo(0);
    }

    private void initData() {
        pro_name = getIntent().getStringExtra("pro_name");
        pro_id = getIntent().getIntExtra("pro_id", -1);

    }

    @OnClick({ R.id.pro_tv_btn_item1, R.id.pro_tv_btn_item2, R.id.pro_tv_btn_item3, R.id.pro_tv_btn_item4, R.id.pro_tv_btn_item5})
    public void onClick(View view) {
        switch (view.getId()) {
            case R.id.pro_tv_btn_item1:
                transactionTo(0);
                break;
            case R.id.pro_tv_btn_item2:
                transactionTo(1);
                break;
            case R.id.pro_tv_btn_item3:
                transactionTo(2);
                break;
            case R.id.pro_tv_btn_item4:
                transactionTo(3);
                break;
            case R.id.pro_tv_btn_item5:
                transactionTo(4);
                break;
        }
    }

    private void transactionTo(int position) {
        if (fragmentManager == null) {
            fragmentManager = getSupportFragmentManager();
        }

        fragmentManager.beginTransaction().replace(R.id.pro_fl_content, pro_lt_fragment.get(position)).commit();

    }
}