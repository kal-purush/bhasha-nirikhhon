package com.ray.note.activity;

import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.widget.ListView;

import com.ray.api.activity.AlbumActivity;
import com.ray.api.activity.BaseActivity;
import com.ray.api.helper.adapter.IMultiItemTypeSupport;
import com.ray.api.view.pullrefresh.ui.PullToRefreshBase;
import com.ray.api.view.pullrefresh.ui.PullToRefreshListView;
import com.ray.note.R;
import com.ray.note.adapter.ExtendHelperListViewAdapter;
import com.ray.note.task.GetDataTask;

import java.util.Arrays;
import java.util.LinkedList;


public class HelperListViewActivity extends BaseActivity implements IMultiItemTypeSupport,
        PullToRefreshBase.OnRefreshListener, View.OnClickListener
        , GetDataTask.IPostExcuteListener {

    private ListView mListView;
    private PullToRefreshListView mPullListView;


//    private HelperListViewAdapter mAdapter;
    private ExtendHelperListViewAdapter mAdapter;


    private LinkedList<String> mListItems;
    private boolean mIsStart = true;
    private int mCurIndex = 0;
    private static final int mLoadDataCount = 20;

    private GetDataTask mDataTask;

    @Override
    public void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
    }

    @Override
    protected void initView() {
        setContentView(R.layout.activity_listview);
        mPullListView = (PullToRefreshListView) findViewById(R.id.ptl_list);

        mPullListView.setPullLoadEnabled(false);
        mPullListView.setScrollLoadEnabled(true);
        mPullListView.setOnRefreshListener(this);
    }

    @Override
    protected void initData() {
        mCurIndex = mLoadDataCount;
        mListItems = new LinkedList<String>();
        mListItems.addAll(Arrays.asList(GetDataTask.mStrings).subList(0, mCurIndex));

        mListView = mPullListView.getRefreshableView();

//        mAdapter = new HelperListViewAdapter(this,
//                android.R.layout.simple_list_item_1, mListItems);
//        mListView.setAdapter(mAdapter);
//        mAdapter.setOnClickListener(this);
        mAdapter = new ExtendHelperListViewAdapter(this, mListItems, this);
        mListView.setAdapter(mAdapter);

    }

    @Override
    protected void initListener() {
        mAdapter.setOnClickListener(this);
    }

    @Override
    public int getLayoutId(int position, Object o) {
        if (o.toString().contains("A")) {
            return android.R.layout.simple_expandable_list_item_2;
        }
        return android.R.layout.simple_expandable_list_item_1;
    }

    @Override
    public int getViewTypeCount() {
        return 2;
    }

    @Override
    public int getItemViewType(int position, Object o) {
        if (o.toString().contains("A")) {
            return 0;
        }
        return 1;
    }

    @Override
    public void onPullDownToRefresh(PullToRefreshBase refreshView) {
        mIsStart = true;
        mDataTask = new GetDataTask();
        mDataTask.setPostExcuteListener(this);
        mDataTask.execute();
    }

    @Override
    public void onPullUpToRefresh(PullToRefreshBase refreshView) {
        mIsStart = false;
        mDataTask = new GetDataTask();
        mDataTask.setPostExcuteListener(this);
        mDataTask.execute();
    }

    @Override
    public void onClick(View v) {
        switch (v.getId()) {
            case android.R.id.text1:
//                ToastUtil.getInstance(this).showToast(v.getTag().toString());
                startActivity(new Intent(mContext, AlbumActivity.class));
                break;
        }
    }

    @Override
    public void onPostExcuted(boolean isSuccess) {

        mPullListView.onPullDownRefreshComplete();
        mPullListView.onPullUpRefreshComplete();
        if (isSuccess) {
            boolean hasMoreData = true;
            if (mIsStart) {
                mAdapter.add(0, "Added after refresh..." + mAdapter.getCount());
            } else {
                int start = mCurIndex;
                int end = mCurIndex + mLoadDataCount;
                if (end >= GetDataTask.mStrings.length) {
                    end = GetDataTask.mStrings.length;
                    hasMoreData = false;
                }

                for (int i = start; i < end; ++i) {
                    mAdapter.add(GetDataTask.mStrings[i], false);
                }

                mCurIndex = end;
                mAdapter.notifyDataSetChanged();

            }
            mPullListView.setHasMoreData(hasMoreData);
        }
    }
}