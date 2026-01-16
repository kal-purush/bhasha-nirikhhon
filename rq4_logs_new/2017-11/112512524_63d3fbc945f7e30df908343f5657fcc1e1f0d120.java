package com.example.raulstriglio.dagger2poc.component;

import com.example.raulstriglio.dagger2poc.MyApplication;
import com.example.raulstriglio.dagger2poc.module.AppModule;
import com.example.raulstriglio.dagger2poc.module.ActivityBuilder;

import javax.inject.Singleton;

import dagger.BindsInstance;
import dagger.Component;
import dagger.android.AndroidInjectionModule;

/**
 * Created by raul.striglio on 24/11/17.
 */

@Singleton
@Component(modules = {AndroidInjectionModule.class, AppModule.class, ActivityBuilder.class})
public interface AppComponent {

    @Component.Builder
    interface Builder {

        @BindsInstance
        Builder application(MyApplication application);

        AppComponent build();

    }

    void inject(MyApplication app);

}