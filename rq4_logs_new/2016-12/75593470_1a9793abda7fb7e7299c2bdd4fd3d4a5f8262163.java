package com.example.android_tfw_retrofit2_mvp.activity.login;

import android.animation.Animator;
import android.animation.AnimatorListenerAdapter;
import android.annotation.TargetApi;
import android.content.Intent;
import android.os.Build;
import android.os.Bundle;
import android.support.annotation.NonNull;
import android.text.TextUtils;
import android.view.KeyEvent;
import android.view.View;
import android.view.inputmethod.EditorInfo;
import android.widget.AutoCompleteTextView;
import android.widget.Button;
import android.widget.EditText;
import android.widget.LinearLayout;
import android.widget.ProgressBar;
import android.widget.RadioButton;
import android.widget.ScrollView;
import android.widget.TextView;
import android.widget.Toast;

import com.example.android_tfw_retrofit2_mvp.R;
import com.example.android_tfw_retrofit2_mvp.activity.main.main_navigation_drawer.MainActivity;
import com.example.android_tfw_retrofit2_mvp.api.ApiConfing;
import com.example.android_tfw_retrofit2_mvp.api.AppClient;
import com.example.android_tfw_retrofit2_mvp.base.BaseActivity;

import butterknife.BindView;
import butterknife.ButterKnife;
import butterknife.OnClick;

import static com.google.gson.internal.$Gson$Preconditions.checkNotNull;


/**
 * A login screen that offers login via email/password.
 */
public class LoginActivity extends BaseActivity implements LoginContract.View {

    // UI references.

    @BindView(R.id.login_progress)
    ProgressBar mProgressView;
    @BindView(R.id.username)
    AutoCompleteTextView mUserName;
    @BindView(R.id.password)
    EditText mPasswordView;
    @BindView(R.id.sign_in_button)
    Button signInButton;
    @BindView(R.id.username_login_form)
    LinearLayout usernameLoginForm;
    @BindView(R.id.login_form)
    ScrollView mLoginFormView;

    LoginContract.Presenter loginPresenter;
    @BindView(R.id.id_rb_testNet)
    RadioButton idRbTestNet;
    @BindView(R.id.id_rb_formalNet)
    RadioButton idRbFormalNet;
    @BindView(R.id.btn_register)
    Button btnRegister;

//    @Override
//    public void onRegisterCloseListener() {
//        registerCloseListener(this);
//    }
//
//    @Override
//    public void onUnRegisterCloseListener() {
//        unRegisterCloseListener(this);
//    }

    @Override
    protected void onResume() {
        super.onResume();
        registerCloseListener(this);
    }

    @Override
    protected void onDestroy() {
        super.onDestroy();
        unRegisterCloseListener(this);
    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_login);
        ButterKnife.bind(this);

        mPasswordView.setOnEditorActionListener(new TextView.OnEditorActionListener() {
            @Override
            public boolean onEditorAction(TextView textView, int id, KeyEvent keyEvent) {
                if (id == R.id.login || id == EditorInfo.IME_NULL) {
                    attemptLogin();
                    return true;
                }
                return false;
            }
        });

        //默认用户
        mUserName.setText("lijun02");
        mPasswordView.setText("123456");


        //初始化loginpresenter
        new LoginPresenter(this, this);

    }

    /**
     * Attempts to sign in or register the account specified by the login form.
     * If there are form errors (invalid email, missing fields, etc.), the
     * errors are presented and no actual login attempt is made.
     */
    private void attemptLogin() {
        // Reset errors.
        mUserName.setError(null);
        mPasswordView.setError(null);

        // Store values at the time of the login attempt.
        String email = mUserName.getText().toString();
        String password = mPasswordView.getText().toString();

        boolean cancel = false;
        View focusView = null;

        // Check for a valid password, if the user entered one.
        if (!TextUtils.isEmpty(password) && !isPasswordValid(password)) {
            mPasswordView.setError(getString(R.string.error_invalid_password));
            focusView = mPasswordView;
            cancel = true;
        }


        if (cancel) {
            // There was an error; don't attempt login and focus the first
            // form field with an error.
            focusView.requestFocus();
        } else {
            // Show a progress spinner, and kick off a background task to
            // perform the user login attempt.
            showProgress(true);
            // TODO: 2016/11/14  login
            loginPresenter.toLogin(email, password);
        }
    }

    @OnClick({R.id.id_rb_testNet, R.id.id_rb_formalNet, R.id.sign_in_button})
    public void onClick(View view) {
        switch (view.getId()) {
            case R.id.id_rb_testNet:
                AppClient.retrofit = null;
                ApiConfing.initBaseUrl(true);
                break;
            case R.id.id_rb_formalNet:
                AppClient.retrofit = null;
                ApiConfing.initBaseUrl(false);
                break;
            case R.id.sign_in_button:
                // TODO: 2016/11/14  登陆
                attemptLogin();
                break;

        }
    }

    @Override
    public void setPresenter(@NonNull LoginContract.Presenter presenter) {
        this.loginPresenter = checkNotNull(presenter); // 非null 判断
    }

    @Override
    public void showErro(int a, String msg) {
        showProgress(false);
        showToast(msg);
    }


    @Override
    public void toMianActivity() {
        showProgress(false);
        showToast("登陆成功");
        startActivity(new Intent(this, MainActivity.class));
    }

    private boolean isEmailValid(String email) {
        //TODO: Replace this with your own logic
        return email.contains("@");
    }

    private boolean isPasswordValid(String password) {
        //TODO: Replace this with your own logic
        return password.length() > 4;
    }

    /**
     * Shows the progress UI and hides the login form.
     */
    @TargetApi(Build.VERSION_CODES.HONEYCOMB_MR2)
    public void showProgress(final boolean show) {
        // On Honeycomb MR2 we have the ViewPropertyAnimator APIs, which allow
        // for very easy animations. If available, use these APIs to fade-in
        // the progress spinner.
        if (Build.VERSION.SDK_INT >= Build.VERSION_CODES.HONEYCOMB_MR2) {
            int shortAnimTime = getResources().getInteger(android.R.integer.config_shortAnimTime);

//            mLoginFormView.setVisibility(show ? View.GONE : View.VISIBLE);
//            mLoginFormView.animate().setDuration(shortAnimTime).alpha(
//                    show ? 0 : 1).setListener(new AnimatorListenerAdapter() {
//                @Override
//                public void onAnimationEnd(Animator animation) {
////                    mLoginFormView.setVisibility(show ? View.GONE : View.VISIBLE);
//                }
//            });

            mProgressView.setVisibility(show ? View.VISIBLE : View.GONE);
            mProgressView.animate().setDuration(shortAnimTime).alpha(
                    show ? 1 : 0).setListener(new AnimatorListenerAdapter() {
                @Override
                public void onAnimationEnd(Animator animation) {
                    mProgressView.setVisibility(show ? View.VISIBLE : View.GONE);
                }
            });
        } else {
            // The ViewPropertyAnimator APIs are not available, so simply show
            // and hide the relevant UI components.
            mProgressView.setVisibility(show ? View.VISIBLE : View.GONE);
//            mLoginFormView.setVisibility(show ? View.GONE : View.VISIBLE);
        }
    }
    /**
     * 菜单、返回键响应
     */
    @Override
    public boolean onKeyDown(int keyCode, KeyEvent event) {
        if (keyCode == KeyEvent.KEYCODE_BACK) {
            exitBy2Click();
            return true;
        }
        return super.onKeyDown(keyCode, event);
    }
    long mExitTime;

    /**
     * 双击退出函数
     */
    public void exitBy2Click() {
        //双击退出函数
        if (System.currentTimeMillis() - mExitTime > 2000){
            Toast.makeText(this, "再按一次退出程序", Toast.LENGTH_SHORT).show();
            mExitTime = System.currentTimeMillis();
        }else {
//                    System.exit(0);
            application.exitSystem();
        }
    }

}
