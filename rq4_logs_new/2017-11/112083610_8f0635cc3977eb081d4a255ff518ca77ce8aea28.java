package com.tline.android.features.login;

import com.tline.android.features.login.interactor.LoginInteractor;
import com.tline.android.features.login.presenter.LoginPresenter;
import com.tline.android.features.login.presenter.impl.LoginPresenterImpl;
import com.tline.android.features.login.view.LoginView;

import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.junit.MockitoJUnit;
import org.mockito.junit.MockitoRule;

import static org.mockito.Mockito.verify;

/**
 * @author Naeem <naeemark@gmail.com>
 * @version 1.0.0
 * @since 29/11/2017
 */


public class LoginPresenterTest {

    @Rule
    public MockitoRule mockitoRule = MockitoJUnit.rule();

    @Mock
    private LoginInteractor mInteractor;

    @Mock
    private LoginView mView;

    private LoginPresenter mPresenter;

    @Before
    public void setUp() throws Exception {
        mPresenter = new LoginPresenterImpl(mInteractor);
        mPresenter.onViewAttached(mView);
    }

    @Test
    public void shouldshowLoginUi() throws Exception {
        mPresenter.checkSession(null);

        verify(mView).showLoginUi();
    }

    @Test
    public void shouldLogout() throws Exception {
        mPresenter.logout();

        verify(mView).logoutTwitter();
    }

}