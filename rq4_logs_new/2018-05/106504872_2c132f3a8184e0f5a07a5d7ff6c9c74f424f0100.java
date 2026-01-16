package com.entrevista.ifood2.dagger.module;

import com.entrevista.ifood2.dagger.scope.PerActivityScope;
import com.entrevista.ifood2.presentation.presenter.cart.CartView;

import dagger.Module;
import dagger.Provides;

/**
 * {Created by Jonatas Caram on 21/05/2018}.
 */
@Module
public class CartModule {
    private CartView mCartView;

    public CartModule(CartView mCartView) {
        this.mCartView = mCartView;
    }

    @Provides
    @PerActivityScope
    CartView provideCartView() {
        return mCartView;
    }
}