package com.example.unic_1.mvptest.presenter;

import com.example.unic_1.mvptest.contractor.MainContract;

import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

import static org.junit.Assert.*;
import static org.mockito.Mockito.verify;

/**
 * Local unit test for the main presenter
 */
public class MainPresenterTest {

    @Mock
    private MainContract.MainView mView;

    private MainPresenter mainPresenter;

    @Before
    public void setUp() throws Exception {
        MockitoAnnotations.initMocks(this);
        mainPresenter = new MainPresenter(mView);
    }

    @Test
    public void handleSignInButtonClick() {
        mainPresenter.handleSignInButtonClick();
        verify(mView).showSignInScreen();
    }

    @Test
    public void handleSignUpButtonClick() {
        mainPresenter.handleSignUpButtonClick();
        verify(mView).showSignUpScreen();
    }
}