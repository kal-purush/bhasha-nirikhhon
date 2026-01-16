package com.abt.basic.util;

import android.view.Gravity;
import android.widget.Toast;

import com.abt.basic.BasicApplication;

/**
 * @描述： @ToastUtil
 * @作者： @黄卫旗
 * @创建时间： @20/05/2018
 */
public class ToastUtil {

    private static Toast toast;

    /**
     * show toast
     * @param msg     message string
     */
    public static void show(String msg) {
        if (toast == null) {
            toast = Toast.makeText(BasicApplication.getAppContext(), "", Toast.LENGTH_SHORT);
        }
        toast.setText(msg);
        toast.setGravity(Gravity.CENTER, 0, 0);
        toast.show();
    }

    /**
     * show toast
     * @param msgId   message resource id
     */
    public static void show(int msgId) {
        if (toast == null) {
            toast = Toast.makeText(BasicApplication.getAppContext(), "", Toast.LENGTH_SHORT);
        }
        toast.setText(msgId);
        toast.setGravity(Gravity.CENTER, 0, 0);
        toast.show();
    }

}