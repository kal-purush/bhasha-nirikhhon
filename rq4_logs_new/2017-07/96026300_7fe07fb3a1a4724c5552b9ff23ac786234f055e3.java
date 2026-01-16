package io.happylrd.daphne.activity;

import android.content.Context;
import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;

import butterknife.BindView;
import io.happylrd.daphne.R;
import io.happylrd.youbo.common.app.Activity;

/**
 * <h1>modify class name here</h1>
 * io.happylrd.daphne.activity
 * <p>
 * add description here
 *
 * @author Crepusculo
 * @version 1.0
 * @since 2017/7/6
 */

public class EntranceActivity extends Activity implements View.OnClickListener{
    @BindView(R.id.login_btn)
    Button loginBtn;

    @Override
    protected int getContentLayout() {
        return R.layout.activity_entrance;
    }

    @Override
    protected void initWidget() {
        super.initWidget();
        loginBtn.setOnClickListener(this);
    }

    @Override
    public void onClick(View view) {
        Intent intent = new Intent().setClass(this,MainActivity.class);
        this.startActivity(intent);
        finish();
    }
}