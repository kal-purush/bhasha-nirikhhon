package com.example.wangning.scrollview;

import android.app.Activity;
import android.os.Bundle;
import android.os.Handler;
import android.widget.ArrayAdapter;
import android.widget.ListView;

import com.example.wangning.R;
import com.example.wangning.utils.ViewUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * Created by Administrator on 2018/4/9.
 */
public class ScrollListViewActivity extends Activity {

    private ListView lv;
    private List<String> mList = new ArrayList<>();
    private ArrayAdapter mAdapter;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_scroll_listview);
        lv = (ListView) findViewById(R.id.lv);
        mAdapter = new ArrayAdapter(this, android.R.layout.simple_list_item_1, mList);
        lv.setAdapter(mAdapter);
        new Handler().postDelayed(new Runnable() {
            @Override
            public void run() {
                fillData();
            }
        }, 1000);

    }

    private void fillData() {
        for (int i = 0; i < 10; i++) {
            mList.add("A" + i);
        }
        ViewUtils.setListViewHeight(lv);
        mAdapter.notifyDataSetChanged();
    }
}