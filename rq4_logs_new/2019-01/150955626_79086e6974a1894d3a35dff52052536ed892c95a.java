package wangfeixixi.cip.ui.functions;

import android.view.View;

import wangfeixixi.cip.R;
import wangfeixixi.cip.fram.BaseActivity;

public class FunctionActivity extends BaseActivity<FunctionDelegate> implements View.OnClickListener {

    @Override
    protected Class<FunctionDelegate> getDelegateClass() {
        return FunctionDelegate.class;
    }

    @Override
    protected void onResume() {
        super.onResume();
        viewDelegate.setOnClickListener(this, R.id.btn_bird, R.id.btn_map);
    }

    @Override
    public void onClick(View v) {
        switch (v.getId()) {
            case R.id.btn_bird:
                viewDelegate.birdOnClick();
                break;
            case R.id.btn_map:
                viewDelegate.mapOnClick();
                break;
        }
    }
}