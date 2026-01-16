package com.psedoykin.birthday;

import android.app.Application;

import javax.inject.Singleton;

import dagger.Component;


@Singleton
@Component(modules = ApplicationModule.class)
public interface ApplicationComponent {

    Application getApplication();

    PresenterManager getPresenterManager();
}