package com.example.administrator.base;

import android.app.Activity;
import android.app.Dialog;
import android.content.Context;
import android.content.res.Resources;
import android.os.Bundle;
import android.view.LayoutInflater;

import com.example.administrator.dialog.LoadingBlack;
import com.example.administrator.txtread.R;

/**
 * Activity基类
 */
public abstract class CommonBaseActivity extends Activity {

    /**
     * Resources
     */
    protected Resources mRes;

    /**
     * Context
     */
    protected Context mCt;

    /**
     * LayoutInflater
     */
    protected LayoutInflater mLif;

    /* 提示框对象 */
    private Dialog mDialog;

    /**
     * 提示框类型——加载
     */
    protected static final int DIALOG_KEY_LOAD = 0;

    private boolean mCancelable = false;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        mCt = this.getApplicationContext();
        mRes = getResources();
        mLif = LayoutInflater.from(mCt);
    }

    @Override
    protected Dialog onCreateDialog(int id) {
        switch (id) {
            case DIALOG_KEY_LOAD:
                mDialog = new LoadingBlack(this, R.style.DialogBlack, getString(R.string.common_loadding));
                mDialog.setCancelable(mCancelable);
                mDialog.setCanceledOnTouchOutside(false);
                return mDialog;
            default:
                return null;
        }
    }

    /**
     * 弹出自定义对话框
     *
     * @param cancelable
     */
    protected void showSelfDefineDialog(boolean cancelable) {
        mCancelable = cancelable;
        showDialog(DIALOG_KEY_LOAD);
    }

    /**
     * 隐藏自定义对话框
     */
    protected void removeSelfDefineDialog() {
        removeDialog(DIALOG_KEY_LOAD);
    }
}