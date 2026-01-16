package wangfeixixi.cip;

import android.view.View;

import com.alibaba.android.arouter.launcher.ARouter;

import wangfeixixi.com.commen.constant.ArouterUrlConstant;
import wangfeixixi.com.commen.fram.BaseActivity;


public class MainActivity extends BaseActivity<MainDelegate> implements View.OnClickListener {
    @Override
    protected Class<MainDelegate> getDelegateClass() {
        return MainDelegate.class;
    }

    @Override
    protected void onResume() {
        super.onResume();

        viewDelegate.setOnClickListener(this, R.id.btn_map, R.id.btn_bird);

    }

    @Override
    public void onClick(View v) {
        switch (v.getId()) {
            case R.id.btn_bird:
                ARouter.getInstance().build(ArouterUrlConstant.BIRD).navigation();
                break;
            case R.id.btn_map:
                ARouter.getInstance().build(ArouterUrlConstant.MAP).navigation();
                break;
        }
    }
}