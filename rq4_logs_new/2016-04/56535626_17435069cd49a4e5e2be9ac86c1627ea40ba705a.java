package com.numan1617.tfltubedepartures.base;

import android.os.Bundle;
import android.support.annotation.LayoutRes;
import android.support.annotation.NonNull;
import android.support.v7.app.AppCompatActivity;
import butterknife.ButterKnife;

public abstract class BaseActivity<V extends PresenterView> extends AppCompatActivity {

  @Override protected void onCreate(final Bundle savedInstanceState) {
    super.onCreate(savedInstanceState);
    setContentView(getLayoutId());
    ButterKnife.bind(this);
  }

  @LayoutRes protected abstract int getLayoutId();

  @NonNull protected abstract BasePresenter<V> getPresenter();

  @NonNull protected abstract V getPresenterView();

  @Override protected void onDestroy() {
    super.onDestroy();
    getPresenter().onViewDetached();
  }
}