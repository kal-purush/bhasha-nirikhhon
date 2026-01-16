package qiuchenly.JSAHVCSC.Base;

import android.app.AlertDialog;
import android.content.DialogInterface;
import android.os.Bundle;
import android.support.annotation.Nullable;
import android.support.v7.app.*;
import android.view.*;

import qiuchenly.JSAHVCSC.Base.iSetting.Sets;

/**
 * Author        : QiuChenLy
 * Times         : 2017 年 10 月 29 日 15:57
 * Usage         : 基本App类，所有可见的Activity必须继承本类
 */

public abstract class BaseApp extends AppCompatActivity implements View.OnClickListener {
    Sets DefaultSets = null;

    @Override
    protected void onCreate(@Nullable Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        DefaultSets = getDefaultSet(new Sets());

        if(DefaultSets.ViewID<=0){
            new AlertDialog.Builder(this)
                    .setTitle("WARN!")
                    .setMessage("设置ViewID错误！请检查代码！")
                    .setPositiveButton("好", new DialogInterface.OnClickListener() {
                        @Override
                        public void onClick(DialogInterface dialogInterface, int i) {
                            System.exit(0);
                        }
                    }).show();
        }else{
            setContentView(DefaultSets.ViewID);
        }


        if (DefaultSets.hideTitleBar) {
            ActionBar actionBar = getSupportActionBar();
            if (actionBar != null && actionBar.isShowing()) {
                actionBar.hide();
            }
        }

        if (DefaultSets.allowStatusBarTranslate) {
            getWindow().getDecorView().setSystemUiVisibility(View.SYSTEM_UI_FLAG_LAYOUT_FULLSCREEN);
        }


    }

    public abstract Sets getDefaultSet(Sets sets);

    public <T extends View> T find(int ID) {
        return findViewById(ID);
    }

    public <T extends View> T find(int ID, boolean allowClick) {
        T t = findViewById(ID);
        if (allowClick) t.setOnClickListener(this);
        return t;
    }

    public <T extends View> T find(View view, int ID) {
        return view.findViewById(ID);
    }

    public <T extends View> T find(View view, int ID, boolean allowClick) {
        T t = view.findViewById(ID);
        if (allowClick) t.setOnClickListener(this);
        return t;
    }


    @Override
    public boolean onKeyDown(int keyCode, KeyEvent event) {
        if(keyCode == KeyEvent.KEYCODE_BACK && DefaultSets.NullBack){
            return true;
        }
        return super.onKeyDown(keyCode, event);
    }

    @Override
    public abstract void onClick(View v);
}