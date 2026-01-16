package com.club.sports.sportsclub.login;

import android.content.Context;
import android.content.Intent;
import android.graphics.Color;
import android.os.Bundle;
import android.support.v7.app.AppCompatActivity;
import android.text.Editable;
import android.text.TextWatcher;
import android.view.View;
import android.view.inputmethod.InputMethodManager;
import android.widget.EditText;
import android.widget.LinearLayout;
import android.widget.RelativeLayout;
import android.widget.TextView;

import com.club.sports.sportsclub.R;
import com.club.sports.sportsclub.back.BackPressCloseHandler;
import com.club.sports.sportsclub.home.SportsClubHomeActivity;

/**
 * Created by again on 2017-09-23.
 */

public class LoginActivity extends AppCompatActivity {

    private BackPressCloseHandler mBackPressCloseHandler;
    private LinearLayout mLoginLinearLayout;
    private RelativeLayout mLoginRelativeLayout;
    private RelativeLayout mMemberRelativeLayout;
    private TextView mIdTextView;
    private TextView mPasswordTextView;
    private EditText mIdEditText;
    private EditText mPasswordEditText;
    private InputMethodManager mInputMethodManager;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.sportsclub_login);

        initialize();
    }

    private void initialize() {
        mBackPressCloseHandler = new BackPressCloseHandler(this);
        findView();
        onClickLoginButton();
        onClickMemberButton();
        onTouchLoginLinearLayout();
        onAddTextChangedListener();
    }

    private void findView() {
        mLoginLinearLayout = (LinearLayout) findViewById(R.id.login_layout);
        mLoginRelativeLayout = (RelativeLayout) findViewById(R.id.login_relative_layout);
        mMemberRelativeLayout = (RelativeLayout) findViewById(R.id.member_relative_layout);
        mIdTextView = (TextView) findViewById(R.id.id_textview);
        mPasswordTextView = (TextView) findViewById(R.id.password_textview);
        mIdEditText = (EditText) findViewById(R.id.id_edittext);
        mPasswordEditText = (EditText) findViewById(R.id.password_edittext);
    }

    private void onClickLoginButton(){
        mLoginRelativeLayout.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent intent = new Intent().setClass(LoginActivity.this, SportsClubHomeActivity.class);
                intent.putExtra("login_info", "login_activity");
                startActivity(intent);
                finish();
            }
        });
    }

    private void onClickMemberButton(){
        mMemberRelativeLayout.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {

            }
        });
    }

    private void onTouchLoginLinearLayout() {
        mLoginLinearLayout.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                hideIdKeyboard();
                hidePasswordKeyboard();
            }
        });
    }

    private void onAddTextChangedListener() {

        mIdEditText.addTextChangedListener(new TextWatcher() {
            @Override
            public void beforeTextChanged(CharSequence s, int start, int count, int after) {
                if (checkIdEditText()) {
                    mIdTextView.setVisibility(View.VISIBLE);
                    return;
                }
                mIdTextView.setVisibility(View.GONE);
            }

            @Override
            public void onTextChanged(CharSequence s, int start, int before, int count) {

            }

            @Override
            public void afterTextChanged(Editable s) {
                if (checkIdEditText()) {
                    mIdTextView.setVisibility(View.VISIBLE);
                    return;
                }
                mIdTextView.setVisibility(View.GONE);
            }
        });

        mPasswordEditText.addTextChangedListener(new TextWatcher() {
            @Override
            public void beforeTextChanged(CharSequence s, int start, int count, int after) {
                if (checkPasswordEditText()) {
                    mPasswordTextView.setVisibility(View.VISIBLE);
                    return;
                }
                mPasswordTextView.setVisibility(View.GONE);
            }

            @Override
            public void onTextChanged(CharSequence s, int start, int before, int count) {

            }

            @Override
            public void afterTextChanged(Editable s) {
                if (checkPasswordEditText()) {
                    mPasswordTextView.setVisibility(View.VISIBLE);
                    return;
                }
                mPasswordTextView.setVisibility(View.GONE);
            }
        });
    }

    private boolean checkIdEditText() {
        return (mIdEditText.getText().length() == 0);
    }

    private boolean checkPasswordEditText() {
        return (mPasswordEditText.getText().length() == 0);
    }

    private void hideIdKeyboard() {
        mInputMethodManager = (InputMethodManager) getSystemService(Context.INPUT_METHOD_SERVICE);
        mInputMethodManager.hideSoftInputFromWindow(mIdEditText.getWindowToken(), 0);
    }

    private void hidePasswordKeyboard() {
        mInputMethodManager = (InputMethodManager) getSystemService(Context.INPUT_METHOD_SERVICE);
        mInputMethodManager.hideSoftInputFromWindow(mPasswordEditText.getWindowToken(), 0);
    }

    @Override
    public void onBackPressed() {
        mBackPressCloseHandler.onBackPressed();
    }
}