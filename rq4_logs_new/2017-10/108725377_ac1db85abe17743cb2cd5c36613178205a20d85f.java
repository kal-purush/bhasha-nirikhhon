package qiuchenly.JSAHVCSC.UIViews;

import android.view.View;

import qiuchenly.JSAHVCSC.Base.BaseApp;
import qiuchenly.JSAHVCSC.Base.iSetting.Sets;
import qiuchenly.JSAHVCSC.R;

public class Login extends BaseApp {

    @Override
    public void findID() {

    }

    @Override
    public void resolveFinish() {

    }

    @Override
    public Sets getDefaultSet(Sets sets) {
        sets.ViewID = R.layout.view_login;
        sets.hideTitleBar = true;
        sets.allowStatusBarTranslate = true;
        return sets;
    }

    @Override
    public void onClick(View v) {

    }
}