package com.smapley.powerwork.http;

import android.content.Context;
import android.content.Intent;
import android.content.SharedPreferences;

import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.TypeReference;
import com.lidroid.xutils.exception.HttpException;
import com.lidroid.xutils.http.ResponseInfo;
import com.lidroid.xutils.http.callback.RequestCallBack;
import com.smapley.powerwork.R;
import com.smapley.powerwork.activity.BaseActivity;
import com.smapley.powerwork.activity.Login;
import com.smapley.powerwork.application.LocalApplication;
import com.smapley.powerwork.entity.Result_Entity;
import com.smapley.powerwork.entity.User_Entity;
import com.smapley.powerwork.utils.ActivityStack;
import com.smapley.powerwork.utils.MyData;

import cn.pedant.SweetAlert.SweetAlertDialog;

/**
 * Created by smapley on 15/11/20.
 */
public abstract class HttpCallBack extends RequestCallBack<String> {

    private SweetAlertDialog dialog;
    private Context context;

    public HttpCallBack(Context context, SweetAlertDialog dialog) {

        this.context = context;
        this.dialog = dialog;
    }

    @Override
    public void onStart() {
        dialog.changeAlertType(SweetAlertDialog.PROGRESS_TYPE);
        dialog.show();
    }

    @Override
    public void onSuccess(ResponseInfo<String> responseInfo) {
        Result_Entity result_entity = JSON.parseObject(responseInfo.result, new TypeReference<Result_Entity>() {
        });
        if (MyData.SUCC.equals(result_entity.getFlag())) {
            this.onResult(result_entity.getData());
        } else if (MyData.OutLogin.equals(result_entity.getFlag())) {
            dialog.changeAlertType(SweetAlertDialog.ERROR_TYPE)
                    .showText(result_entity.getDetails())
                    .showConfirmButton(R.string.login)
                    .showCancelButton()
                    .commit().setOnSweetClickListener(new SweetAlertDialog.OnSweetClickListener() {
                @Override
                public void onConfirmClick(SweetAlertDialog dialog) {
                    SharedPreferences.Editor editor = LocalApplication.getInstance().sp_user.edit();
                    editor.putBoolean("islogin", false);
                    editor.commit();
                    ActivityStack.getInstance().finishAllActivity();
                    context.startActivity(new Intent(context, Login.class));
                    dialog.dismiss();
                }

                @Override
                public void onFirstClick(SweetAlertDialog dialog) {

                }

                @Override
                public void onCancelClick(SweetAlertDialog dialog) {
                    dialog.dismiss();
                }
            });
        }else{
            dialog.changeAlertType(SweetAlertDialog.ERROR_TYPE)
                    .showText(result_entity.getDetails())
                    .showCancelButton()
                    .commit();
        }
    }

    @Override
    public void onFailure(HttpException error, String msg) {
        error.printStackTrace();
        dialog.changeAlertType(SweetAlertDialog.ERROR_TYPE)
                .showText(R.id.connect_fai)
                .commit()
                .dismiss(2000);
    }

    public abstract void onResult(String result);
}