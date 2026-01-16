package com.entrevista.ifood2.dagger.component;

import com.entrevista.ifood2.dagger.module.MenuModule;
import com.entrevista.ifood2.dagger.scope.PerFragmentScope;
import com.entrevista.ifood2.presentation.ui.menu.MenuFragment;

import dagger.Component;

/**
 * Created by Jonatas Caram on 23/05/2018.
 * for Bode's Lab in www.bodeslab.com
 */
@PerFragmentScope
@Component(
        dependencies = AppComponent.class,
        modules = MenuModule.class
)
public interface MenuComponent {
    void inject(MenuFragment fragment);
}