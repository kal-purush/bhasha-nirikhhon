package com.andygu.sample_02.activity;

import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import com.andygu.sample_02.R;

public class FragmentActivity extends AppCompatActivity {

  @Override protected void onCreate(Bundle savedInstanceState) {
    super.onCreate(savedInstanceState);
    setContentView(R.layout.activity_fragment);
  }
}