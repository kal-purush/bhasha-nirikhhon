package com.github.nukc.sample;

import android.os.Bundle;
import android.support.v7.app.AppCompatActivity;
import android.view.Menu;
import android.view.MenuInflater;
import android.view.MenuItem;

import com.github.nukc.stateview.StateView;
import com.github.nukc.stateview.animations.SlideAnimatorProvider;

public class ConstraintLayoutActivity extends AppCompatActivity {

    private StateView mStateView;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_constraint_layout);

        mStateView = StateView.inject(findViewById(R.id.constraint_layout));
        mStateView.setAnimatorProvider(new SlideAnimatorProvider());
        mStateView.setEmptyResource(R.layout.view_empty);
        mStateView.setRetryResource(R.layout.view_retry);
        mStateView.setOnRetryClickListener(new StateView.OnRetryClickListener() {
            @Override
            public void onRetryClick() {
                //do something
                mStateView.showRetry();
            }
        });
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        new MenuInflater(this).inflate(R.menu.menu_inject, menu);
        return super.onCreateOptionsMenu(menu);
    }

    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        switch (item.getItemId()) {
            case R.id.show_empty:
                mStateView.showEmpty();
                break;
            case R.id.show_retry:
                mStateView.showRetry();
                break;
            case R.id.show_loading:
                mStateView.showLoading();
                break;
            case R.id.show_content:
                mStateView.showContent();
                break;
        }
        return super.onOptionsItemSelected(item);
    }
}