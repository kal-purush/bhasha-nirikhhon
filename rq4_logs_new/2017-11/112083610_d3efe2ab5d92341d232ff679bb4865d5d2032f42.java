package com.tline.android.features.login.injection;

import android.support.annotation.NonNull;

import com.tline.android.app.presenter.loader.PresenterFactory;
import com.tline.android.features.login.interactor.LoginInteractor;
import com.tline.android.features.login.interactor.impl.LoginInteractorImpl;
import com.tline.android.features.login.presenter.LoginPresenter;
import com.tline.android.features.login.presenter.impl.LoginPresenterImpl;

import dagger.Module;
import dagger.Provides;

@Module
public final class LoginViewModule {
    @Provides
    public LoginInteractor provideInteractor() {
        return new LoginInteractorImpl();
    }

    @Provides
    public PresenterFactory<LoginPresenter> providePresenterFactory(@NonNull final LoginInteractor interactor) {
        return new PresenterFactory<LoginPresenter>() {
            @NonNull
            @Override
            public LoginPresenter create() {
                return new LoginPresenterImpl(interactor);
            }
        };
    }
}