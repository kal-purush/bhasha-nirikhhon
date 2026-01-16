package cc.tracyzhang.app.tracydemo.constraintlayout;

import android.os.Bundle;
import android.support.annotation.Nullable;
import android.support.v7.app.AppCompatActivity;

import cc.tracyzhang.app.tracydemo.R;

/**
 * Created by zl on 17-7-27.
 */

public class ConstraintActivity extends AppCompatActivity {

  @Override
  protected void onCreate(@Nullable Bundle savedInstanceState) {
    super.onCreate(savedInstanceState);
    setContentView(R.layout.constraint_layout);
  }
}