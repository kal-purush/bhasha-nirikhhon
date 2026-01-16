package np.com.naxa.factsnepal.surveys;

import android.os.Bundle;
import android.support.annotation.Nullable;
import android.view.View;

import np.com.naxa.factsnepal.R;
import np.com.naxa.factsnepal.common.BaseActivity;
import np.com.naxa.factsnepal.utils.ActivityUtil;

public class SurveyEndActivity extends BaseActivity implements View.OnClickListener {

    @Override
    protected void onCreate(@Nullable Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);

        setContentView(R.layout.activity_survey_end);
        setupToolbar();

        bindUI();

    }

    private void bindUI() {
        findViewById(R.id.btn_start_survey).setOnClickListener(this);
    }

    @Override
    public void onClick(View v) {
        switch (v.getId()) {
            case R.id.btn_start_survey:
                ActivityUtil.openActivity(SurveyActivity.class,this,null,false);
                break;
        }
    }
}