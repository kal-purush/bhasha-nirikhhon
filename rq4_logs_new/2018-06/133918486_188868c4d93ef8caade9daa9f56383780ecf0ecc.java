package com.bypay.yifu.activity;

import android.os.Bundle;
import android.support.v4.widget.SwipeRefreshLayout;
import android.support.v4.widget.SwipeRefreshLayout.OnRefreshListener;
import android.support.v7.widget.LinearLayoutManager;
import android.support.v7.widget.RecyclerView;
import android.view.LayoutInflater;
import android.view.View;
import android.view.View.OnClickListener;
import android.widget.LinearLayout;

import com.bypay.yifu.R;
import com.bypay.yifu.adapter.AddressManageAdapter;
import com.bypay.yifu.base.BaseActivity;
import com.bypay.yifu.bean.HomeTabInfo;
import com.psylife.wrmvplibrary.utils.TitleBuilder;

import java.util.ArrayList;
import java.util.List;

import butterknife.BindView;

/**
 * 地址管理
 * Created by tianchao on 2018/4/3.
 */
public class AddressListActivity extends BaseActivity implements OnClickListener {

    @BindView(R.id.swipeLayout_address)
    SwipeRefreshLayout mSwipeRefreshLayout;
    @BindView(R.id.recycler_address_list)
    RecyclerView mRecyclerAddressList;

    LinearLayout linNext;

    private AddressManageAdapter mAddressManageAdapter;

    @Override
    public View getTitleView() {
        return new TitleBuilder(this)
                .setLeftImage(R.drawable.bt_selector_nav_menu)
                .setTitleText("地址管理")
                .setTitleBgRes(R.color.txt_color_f7f7f7)
                .build();
    }

    @Override
    public int getLayoutId() {
        return R.layout.activity_address_list;
    }

    @Override
    public void initView(Bundle savedInstanceState) {
        List<HomeTabInfo> homeTabInfos = new ArrayList<HomeTabInfo>(4);
        for (int i = 0; i < 4; i++) {
            homeTabInfos.add(new HomeTabInfo("1", 1));
        }
        mAddressManageAdapter = new AddressManageAdapter(homeTabInfos);
        mAddressManageAdapter.setFooterView(getFootView());
        mRecyclerAddressList.setLayoutManager(new LinearLayoutManager(this));
        mRecyclerAddressList.setAdapter(mAddressManageAdapter);
        mRecyclerAddressList.setNestedScrollingEnabled(false);

        mSwipeRefreshLayout.setOnRefreshListener(new OnRefreshListener() {
            @Override
            public void onRefresh() {
                mSwipeRefreshLayout.setRefreshing(false);
            }
        });
    }

    @Override
    public void initdata() {

    }

    public View getFootView() {
        View footView = LayoutInflater.from(this).inflate(R.layout.view_address_foot, null);
        linNext = footView.findViewById(R.id.bt_add_address);
        linNext.setOnClickListener(this);
        return footView;
    }

    @Override
    public void onClick(View v) {

    }
}