package com.entrevista.ifood2.dagger.component;

import android.content.Context;

import com.entrevista.ifood2.dagger.module.application.AppModule;
import com.entrevista.ifood2.dagger.module.application.RepositoryModule;
import com.entrevista.ifood2.dagger.module.application.RetrofitModule;
import com.entrevista.ifood2.dagger.module.application.RoomModule;
import com.entrevista.ifood2.dagger.module.application.SharedPrefsModule;
import com.entrevista.ifood2.repository.Repository;

import javax.inject.Singleton;

import dagger.Component;

/**
 * {Created by Jonatas Caram on 21/05/2018}.
 */
@Singleton
@Component(modules = {
        AppModule.class,
        RetrofitModule.class,
        RoomModule.class,
        SharedPrefsModule.class,
        RepositoryModule.class
})
public interface AppComponent {
    void inject(Context application);

    Repository provideRepository();
}