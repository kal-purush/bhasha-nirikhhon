package com.smapley.powerwork.activity;

import android.support.v7.widget.Toolbar;
import android.view.View;
import android.widget.Button;
import android.widget.ImageView;
import android.widget.TextView;
import android.widget.Toast;

import com.lidroid.xutils.view.annotation.ContentView;
import com.lidroid.xutils.view.annotation.ViewInject;
import com.lidroid.xutils.view.annotation.event.OnClick;
import com.smapley.powerwork.R;

import me.nereo.multi_image_selector.bean.Image;

/**
 * Created by smapley on 15/10/24.
 */
@ContentView(R.layout.activity_aboutus)
public class AboutUs extends BaseActivity {

    @ViewInject(R.id.title_tv_name)
    private TextView title_tv_name;

    @ViewInject(R.id.abo_bt_good)
    private Button abo_bt_good;


    @Override
    protected void initParams() {
        title_tv_name.setText(R.string.aboutus);

    }

    @OnClick({R.id.abo_bt_good,R.id.title_iv_back})
    public void onClick(View view) {
        switch (view.getId()) {
            case R.id.title_iv_back:
                finish();
                break;
            case R.id.abo_bt_good:
                showToast("好评~");
                break;
        }
    }


}