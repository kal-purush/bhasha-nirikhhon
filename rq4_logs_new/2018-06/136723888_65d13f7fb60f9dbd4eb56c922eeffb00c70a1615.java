package com.outsource.danding.qingdaoic.base;

import android.app.Activity;
import android.app.ProgressDialog;
import android.content.Intent;
import android.content.pm.ActivityInfo;
import android.graphics.Color;
import android.os.Bundle;
import android.support.v7.widget.Toolbar;
import android.text.TextUtils;
import android.view.MenuItem;
import android.widget.TextView;
import android.widget.Toast;

import com.outsource.danding.qingdaoic.ui.dialog.LoadingDialog;
import com.outsource.danding.qingdaoic.util.UIUtils;
import com.trello.rxlifecycle2.components.support.RxAppCompatActivity;

import butterknife.ButterKnife;
import butterknife.Unbinder;


import com.outsource.danding.qingdaoic.R;

public class BaseActivity extends RxAppCompatActivity {

    public final String TAG = getClass().getSimpleName();
    public Toolbar mToolbar;
    private Unbinder unbinder;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setRequestedOrientation(ActivityInfo.SCREEN_ORIENTATION_PORTRAIT);
        UIUtils.translucentBar(this, Color.TRANSPARENT);//make toolbar transparent?
    }

    @Override
    public void onContentChanged() {
        super.onContentChanged();
        setToolbar();
        unbinder = ButterKnife.bind(this);
    }


    public void setToolbar() {
        mToolbar = (Toolbar) findViewById(R.id.toolbar);
        if (mToolbar != null) {
            setSupportActionBar(mToolbar);
        }
        if (getSupportActionBar() != null) {
            getSupportActionBar().setDisplayHomeAsUpEnabled(true);
            getSupportActionBar().setDisplayShowTitleEnabled(false);
        }
    }


    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        if (item.getItemId() == android.R.id.home) {
            finish();
            return true;
        }
        return super.onOptionsItemSelected(item);
    }


    public void showToast(String msg) {
        if (!TextUtils.isEmpty(msg))
            Toast.makeText(getActivity(), msg, Toast.LENGTH_SHORT).show();
    }


    private ProgressDialog mProgressDialog;

    public void showProgressDialog(String msg) {
        if (mProgressDialog == null) {
            mProgressDialog = new ProgressDialog(this);
            mProgressDialog.setCancelable(false);
        }
        if (TextUtils.isEmpty(msg)) {
            msg = getString(R.string.loading);
        }
        mProgressDialog.setMessage(msg);
        mProgressDialog.show();
    }

    public void cancelProgressDialog() {
        if (mProgressDialog != null) {
            mProgressDialog.cancel();
        }
    }

    private LoadingDialog mLoadingDialog;

    protected void showLoadingDialog() {
        if (mLoadingDialog == null)
            mLoadingDialog = new LoadingDialog(this, R.style.LoadingDialogStyle);
        mLoadingDialog.show();
    }

    protected void cancelLoadingDialog() {
        if (mLoadingDialog != null)
            mLoadingDialog.cancel();
    }
    public void jumpToActivity(Class activity) {
        Intent intent = new Intent(getActivity(), activity);
        startActivity(intent);
    }

    public void jumpToActivity(Intent intent) {
        startActivity(intent);
    }

    @Override
    protected void onTitleChanged(CharSequence title, int color) {
        setTextViewText(R.id.title, title);
    }

    public void setTextViewText(int id, CharSequence text) {
        TextView tv = (TextView) findViewById(id);
        if (tv != null) {
            tv.setText(text);
        }
    }

    public Activity getActivity() {
        return this;
    }

    @Override
    protected void onDestroy() {
        super.onDestroy();
        unbinder.unbind();
    }


}