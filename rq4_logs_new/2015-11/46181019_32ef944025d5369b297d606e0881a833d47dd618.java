package com.smapley.powerwork.activity;

import android.content.Intent;
import android.view.View;
import android.widget.EditText;
import android.widget.ImageView;
import android.widget.TextView;

import com.lidroid.xutils.view.annotation.ContentView;
import com.lidroid.xutils.view.annotation.ViewInject;
import com.lidroid.xutils.view.annotation.event.OnClick;
import com.smapley.powerwork.R;

/**
 * Created by smapley on 15/11/17.
 */
@ContentView(R.layout.activity_newfolder)
public class NewFolder extends BaseActivity {

    @ViewInject(R.id.title_tv_name)
    private TextView title_tv_name;
    @ViewInject(R.id.title_iv_done)
    private ImageView title_iv_done;

    @ViewInject(R.id.nfd_ev_name)
    private EditText nfd_ev_name;

    @Override
    protected void initParams() {
        title_tv_name.setText(R.string.newfolder);
        title_iv_done.setVisibility(View.VISIBLE);
    }

    @OnClick({R.id.title_iv_back, R.id.title_iv_done})
    public void onClick(View view) {
        switch (view.getId()) {
            case R.id.title_iv_back:
                finish();
                break;
            case R.id.title_iv_done:
                checkName();
                break;
        }
    }

    private void checkName() {
        String name = nfd_ev_name.getText().toString();
        if (name != null && !name.isEmpty()) {
            Intent intent = new Intent();
            intent.putExtra("name", name);
            setResult(RESULT_OK, intent);
            finish();
        } else {
            showToast(R.string.nfd_ts_namenull);
        }
    }
}