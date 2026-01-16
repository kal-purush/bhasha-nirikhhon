package com.example.myapplication.presenters;


import com.arellomobile.mvp.InjectViewState;
import com.arellomobile.mvp.MvpPresenter;
import com.example.myapplication.utils.Utils;
import com.example.myapplication.views.RegisterIF;

@InjectViewState
public class RegisterPresenter extends MvpPresenter<RegisterIF> {

    public RegisterPresenter() {

    }

    public void register(String code, boolean checked) {

        // парсить код и высылать как инт на проверку

        if (!checked) {
            getViewState().showErrorMessage("Принять соглашение");
        } else if (code.length() < 4) {
            getViewState().showErrorMessage("Пожалуйста введите код подтверждения");
        } else {
            getViewState().loadMain();
        }
    }

    public void getSmsCode(String phone) {
        if (!Utils.isValidMobile(phone)) {
            getViewState().showErrorMessage("Проверьте номер телефона");
        }

    }
}