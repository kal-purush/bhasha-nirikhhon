package com.hankou.home.view;

import android.os.Environment;
import android.support.design.widget.TabLayout;
import android.support.v4.app.ActivityCompat;
import android.support.v4.widget.NestedScrollView;
import android.support.v7.widget.LinearLayoutManager;
import android.util.Log;
import android.view.View;
import android.widget.LinearLayout;

import com.hankou.R;
import com.hankou.adapter.HomePagerAdapter;
import com.hankou.base.BaseActivity;
import com.hankou.common.model.CommonEntity;
import com.hankou.mine.model.UserEntity;
import com.hankou.utils.HttpUtil;
import com.hankou.utils.network.CommonObserver;
import com.hankou.utils.network.HttpManager;
import com.hankou.utils.views.AutoRecyclerItemDecoration;
import com.hankou.utils.views.AutoRecyclerView;
import com.hankou.utils.views.ScrollShowOrHideView;

import java.io.File;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;

import butterknife.BindView;
import dalvik.system.DexClassLoader;

/**
 * Created by bykj003 on 2016/12/7.
 */

public class TestActivity extends BaseActivity {

    private static final String TAG = "TestActivity";

    @BindView(R.id.recyclerView)
    public AutoRecyclerView mRecyclerView;

    private HomePagerAdapter mAdapter;

    @Override
    public int getLayoutResId() {
        return R.layout.act_test;
    }

    @Override
    public void initViews() {
        mRecyclerView.setLayoutManager(new LinearLayoutManager(this));
        mRecyclerView.addItemDecoration(new AutoRecyclerItemDecoration(1, 20, true));
        mRecyclerView.setHasFixedSize(true);
        mAdapter = new HomePagerAdapter(this, new ArrayList<UserEntity>());
        mRecyclerView.setAdapter(mAdapter);

        mRecyclerView.setTranslationY(300.0f);
    }

    @Override
    public void loadData() {
        getData();
    }

    @Override
    public void initListeners() {
    }

    @Override
    public void showError(String message) {
    }

    public void getData() {
        HttpUtil.getHttpResult(HttpManager.getInstance().getUserApi().getAllUser(),
                new CommonObserver<CommonEntity<List<UserEntity>>>() {
                    @Override
                    public void onNext(CommonEntity<List<UserEntity>> listCommonEntity) {
                        if (listCommonEntity.status) {
                            mAdapter.setData(listCommonEntity.data);
                            mAdapter.addData(listCommonEntity.data);
                            mAdapter.addData(listCommonEntity.data);
                            mAdapter.addData(listCommonEntity.data);
                        } else {
                        }
                    }
                    @Override
                    public void onError(Throwable e) {
                    }
                });
    }
}