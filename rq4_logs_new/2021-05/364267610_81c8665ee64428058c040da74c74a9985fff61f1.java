package com.rosemeire.deconti.elegantreader;

/* ********************************************************************************************** */
/* UDACITY Android Developer NanoDegree Program
/* Created by Rosemeire Deconti on 10/12/2018
/* Based on "XYZ READER" from Udacity Repository and adjusted to attend Rubric Review
/* https://github.com/udacity/xyz-reader-starter-code
/* ********************************************************************************************** */

import android.app.Activity;
import android.app.Application;

import com.rosemeire.deconti.elegantreader.dataImage.DaggerSingletonComponent;

import javax.inject.Inject;

import dagger.android.AndroidInjector;
import dagger.android.DispatchingAndroidInjector;
import dagger.android.HasActivityInjector;
import timber.log.Timber;

/* ************************************************************************************************/
/* **** Dagger
/* ************************************************************************************************/

public class ElegantReader extends Application implements HasActivityInjector {

    @Inject
    DispatchingAndroidInjector<Activity> dispatchingAndroidInjector;

    @Override
    public void onCreate() {
        super.onCreate();

        initSingletonComponent();


        Timber.plant(new Timber.DebugTree());
    }

    private void initSingletonComponent() {
        DaggerSingletonComponent.builder()
                .application(this)
                .build()
                .inject(this);
    }

    @Override
    public AndroidInjector<Activity> activityInjector() {
        return dispatchingAndroidInjector;
    }
}