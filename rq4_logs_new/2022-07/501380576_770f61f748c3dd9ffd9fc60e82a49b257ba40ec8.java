package org.dieschnittstelle.mobile.android.skeleton;

import android.app.Activity;
import android.content.Intent;
import android.os.Bundle;
import android.util.Log;
import android.util.Patterns;
import android.view.View;
import android.view.inputmethod.EditorInfo;
import android.widget.Button;
import android.widget.ProgressBar;

import androidx.activity.result.ActivityResultLauncher;
import androidx.activity.result.contract.ActivityResultContracts;
import androidx.annotation.Nullable;
import androidx.appcompat.app.AppCompatActivity;
import androidx.databinding.DataBindingUtil;

import com.google.android.material.snackbar.Snackbar;
import com.google.android.material.textfield.TextInputEditText;

import org.dieschnittstelle.mobile.android.skeleton.databinding.ActivityLoginUserViewBinding;
import org.dieschnittstelle.mobile.android.skeleton.model.IUserItemOperations;
import org.dieschnittstelle.mobile.android.skeleton.model.RetrofitRemoteUserItemOperations;
import org.dieschnittstelle.mobile.android.skeleton.model.UserItem;
import org.dieschnittstelle.mobile.android.skeleton.util.MADAsyncOperationRunner;

import java.util.regex.Pattern;

public class LoginActivity extends AppCompatActivity implements LoginviewModel {
    private ActivityLoginUserViewBinding loginUserViewBinding;
    private UserItem user;
    private IUserItemOperations operations;
    private MADAsyncOperationRunner operationRunner;
    private String errorStatus;
    private TextInputEditText eMailText;
    private TextInputEditText passwordText;
    private Button loginButton;
    private ActivityResultLauncher<Intent> overviewActivityLauncher;
    //^[0-9]{1,6}$
    private final Pattern PWD
            = Pattern.compile("\\\\d{6}");
    private ProgressBar progressBar;


    @Override
    public void onCreate(@Nullable Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        this.loginUserViewBinding = DataBindingUtil.setContentView(this, R.layout.activity_login_user_view);
        eMailText = findViewById(R.id.uEmail);
        passwordText = findViewById(R.id.uPassword);
        loginButton = findViewById(R.id.btn_login);
        progressBar = findViewById(R.id.progressBar);
        loginButton.setVisibility(View.VISIBLE);
        operationRunner = new MADAsyncOperationRunner(this, progressBar);
        this.operations = new RetrofitRemoteUserItemOperations();
        this.operationRunner = new MADAsyncOperationRunner(this, null);
        this.loginUserViewBinding.setController(this);

    }

    @Override
    public UserItem getUserItem() {
        return this.user;
    }

    @Override
    public boolean onEmailInputChanged() {
        if (this.errorStatus != null) {
            this.errorStatus = null;
            this.loginUserViewBinding.setController(this);
        }
        return true;
    }

    public boolean checkButtonVisibility(Button button) {
        if (this.validateEmail(eMailText) && validatePassword(passwordText)) {
            button.setVisibility(View.VISIBLE);
        }
        this.loginUserViewBinding.setController(this);
        return true;
    }

    @Override
    public boolean onPasswordInputChanged() {
        if (this.errorStatus != null) {
            this.errorStatus = null;
            this.loginUserViewBinding.setController(this);
        }
        return true;
    }

    @Override
    public void onLoginUser(UserItem userItem) {
        this.operations.prepare(new UserItem("steve@byom.de","123456"));
        launchOverview();
        this.operations.authenticateUser(userItem);
    }

    public boolean launchOverview() {
        this.overviewActivityLauncher = registerForActivityResult(
                new ActivityResultContracts.StartActivityForResult(),
                result -> {
                    if (result.getResultCode() == Activity.RESULT_OK) {
                        Intent overViewIntent = new Intent(this, OverviewActivity.class);
                        overviewActivityLauncher.launch(overViewIntent);
                    }
                }
        );

        return true;
    }

    @Override
    public String getErrorStatus() {
        return errorStatus;
    }

    @Override
    public boolean checkEmailFieldInputComplete(View view, int actionId, boolean hasFocus, boolean isCalledFromOnFocusChange) {
        if (isCalledFromOnFocusChange ?
                !hasFocus
                : actionId == EditorInfo.IME_ACTION_DONE || actionId == EditorInfo.IME_ACTION_NEXT) {
            return validateEmail(eMailText);
        }
        return false;
    }

    private boolean validatePassword(TextInputEditText pwdInputText) {
        String pwdText = pwdInputText.getText().toString();
        if (pwdText.isEmpty() || !PWD.matcher(pwdText).matches()) {
            errorStatus = "invalid password";
            this.loginUserViewBinding.setController(this);
            return false;
        } else {
            Log.i("LOGGER", "valid password");
            checkButtonVisibility(loginButton);
            return true;
        }
    }

    private boolean validateEmail(TextInputEditText eMailText) {
        String inputText = eMailText.getText().toString();

        if (inputText.isEmpty() || !Patterns.EMAIL_ADDRESS.matcher(inputText).matches()) {
            errorStatus = "invalid email address";
            this.loginUserViewBinding.setController(this);
            return false;
        } else {
            Log.i("LOGGER", "valid mail");
            return true;
        }

    }

    @Override
    public boolean checkPasswordFieldInputComplete(View view, int actionId, boolean hasFocus, boolean isCalledFromOnFocusChange) {
        if (isCalledFromOnFocusChange ?
                !hasFocus
                : actionId == EditorInfo.IME_ACTION_DONE || actionId == EditorInfo.IME_ACTION_NEXT) {
            return validatePassword(eMailText);
        }
        return false;
    }

    private void showMessage(String msg) {
        Snackbar.make(this.loginUserViewBinding.getRoot(), msg, Snackbar.LENGTH_INDEFINITE).show();
    }

}