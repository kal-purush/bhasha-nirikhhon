package com.idejie.android.aoc.dialog;

import android.app.Dialog;
import android.content.Context;
import android.os.Bundle;
import android.support.annotation.NonNull;
import android.support.annotation.StyleRes;
import android.view.Display;
import android.view.Gravity;
import android.view.View;
import android.view.Window;
import android.view.WindowManager;
import android.widget.Button;
import android.widget.EditText;

import com.idejie.android.aoc.R;

/**
 * Created by sxb on 2017/7/7.
 */

public class PostCommentDialog extends Dialog {
    private EditText contentTxt;
    private Button sentBtn;

    public PostCommentDialog(@NonNull Context context) {
        this(context, R.style.comment_dialog);
    }

    public PostCommentDialog(@NonNull Context context, @StyleRes int themeResId) {
        super(context, themeResId);
        requestWindowFeature(Window.FEATURE_NO_TITLE);

        Window win = getWindow();
        win.setGravity(Gravity.BOTTOM); //显示在底部

        win.getDecorView().setPadding(0, 0, 0, 0);

        WindowManager.LayoutParams lp = win.getAttributes();

        lp.width = 800;

        lp.height = WindowManager.LayoutParams.WRAP_CONTENT;

        win.setAttributes(lp);

        View convertView = getLayoutInflater().inflate(R.layout.comment_txt_view, null);
        win.setContentView(convertView);
//        setContentView(convertView);

        contentTxt = (EditText) convertView.findViewById(R.id.comment_content);
        sentBtn = (Button) convertView.findViewById(R.id.comment_send_btn);
    }

//    onCrea

//    @Override
//    protected void onCreate(Bundle savedInstanceState) {
//        super.onCreate(savedInstanceState);
//        Window win = getWindow();
//        win.setGravity(Gravity.BOTTOM); //显示在底部
//
//        win.getDecorView().setPadding(0, 0, 0, 0);
//
//        WindowManager.LayoutParams lp = win.getAttributes();
//
//        lp.width = WindowManager.LayoutParams.MATCH_PARENT;
//
//        lp.height = WindowManager.LayoutParams.WRAP_CONTENT;
//
//        win.setAttributes(lp);
//
////        WindowManager m = getWindow().getWindowManager();
////        Display d = m.getDefaultDisplay();
////        WindowManager.LayoutParams p = getWindow().getAttributes();
////        p.width = d.getWidth(); //设置dialog的宽度为当前手机屏幕的宽度
////        getWindow().setAttributes(p);
//    }
}