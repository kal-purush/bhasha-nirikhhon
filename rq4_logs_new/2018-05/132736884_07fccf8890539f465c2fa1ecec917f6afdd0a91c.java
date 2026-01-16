package cn.bupt.runningplanner;

import android.content.Intent;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.view.View;
import android.widget.ImageView;
import android.widget.TextView;

public class ModifySex extends AppCompatActivity {
    private ModifySex self = this;
    private ImageView male;
    private ImageView female;

    View.OnClickListener onClickListener = new View.OnClickListener() {
        @Override
        public void onClick(View v) {
            Intent intent = null;
            switch (v.getId()) {
                case R.id.modify_cancel:
                    intent = new Intent();
                    break;
                case R.id.modify_finish:
                    intent = new Intent();
                    changeSex();
                    break;
                case R.id.modify_male:
                    male.setVisibility(ImageView.VISIBLE);
                    female.setVisibility(ImageView.GONE);
                    break;
                case R.id.modify_female:
                    female.setVisibility(ImageView.VISIBLE);
                    male.setVisibility(ImageView.GONE);
                    break;
                default:
                    if (intent != null) {
                        intent.setClass(self, ModifyInfoActivity.class);
                        startActivity(intent);
                        finish();
                    }
                    break;
            }
        }
    };

    private void changeSex() {

    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_modify_sex);
        male = ((ImageView)findViewById(R.id.modify_male_check));
        female = ((ImageView)findViewById(R.id.modify_female_check));
        ((ImageView)findViewById(R.id.modify_male)).setOnClickListener(onClickListener);
        ((ImageView)findViewById(R.id.modify_female)).setOnClickListener(onClickListener);
        ((TextView)findViewById(R.id.modify_cancel)).setOnClickListener(onClickListener);
        ((TextView)findViewById(R.id.modify_finish)).setOnClickListener(onClickListener);
    }
}