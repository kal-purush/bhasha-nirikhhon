package com.operationcodify.cavoid.activities;

import android.content.SharedPreferences;
import android.preference.PreferenceManager;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;

import androidx.annotation.Nullable;
import androidx.leanback.app.OnboardingSupportFragment;

public class MyOnboardingSupportFragment extends OnboardingSupportFragment{
    static final String preferences = "com.operationcodify.cavoid.activities_preferences";

    @Override
    protected int getPageCount() {
        return 1;
    }

    @Override
    protected CharSequence getPageTitle(int pageIndex) {
        return "WARNING!";
    }

    @Override
    protected CharSequence getPageDescription(int pageIndex) {
        return "This data doesn't imply an encounter with covid-19.";
    }

    @Nullable
    @Override
    protected View onCreateBackgroundView(LayoutInflater inflater, ViewGroup container) {
        return null;
    }

    @Nullable
    @Override
    protected View onCreateContentView(LayoutInflater inflater, ViewGroup container) {
        return null;
    }

    @Nullable
    @Override
    protected View onCreateForegroundView(LayoutInflater inflater, ViewGroup container) {
        return null;
    }
    @Override
    protected void onFinishFragment() {
        // User has seen OnboardingSupportFragment, so mark our SharedPreferences
        // flag as completed so that we don't show our OnboardingSupportFragment
        // the next time the user launches the app.
        super.onFinishFragment();
    }
}