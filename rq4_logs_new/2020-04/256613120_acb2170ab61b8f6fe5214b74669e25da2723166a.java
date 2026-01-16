package ua.opu.pnit.mynotepad.login;

import androidx.appcompat.app.AppCompatActivity;
import androidx.lifecycle.ViewModelProvider;

import android.content.Intent;
import android.os.Bundle;
import android.widget.Toast;

import com.google.android.material.textfield.TextInputEditText;

import butterknife.BindView;
import butterknife.ButterKnife;
import butterknife.OnClick;
import ua.opu.pnit.mynotepad.MainActivity;
import ua.opu.pnit.mynotepad.R;

public class LoginActivity extends AppCompatActivity {

    private LoginViewModel model;

    @BindView(R.id.username_et)
    TextInputEditText mUsernameEditText;

    @BindView(R.id.password_et)
    TextInputEditText mPasswordEditText;

    @OnClick(R.id.signup)
    public void signup() {
        String username = mUsernameEditText.getText().toString();
        String password = mPasswordEditText.getText().toString();

        model.loginAttempt(username, password);
    }

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_login);

        ButterKnife.bind(this);

        model = new ViewModelProvider(this, ViewModelProvider.AndroidViewModelFactory.getInstance(this.getApplication())).get(LoginViewModel.class);
        model.getLoginResultLiveData().observe(this, result -> {
            if (result) {
                Intent i = new Intent(this, MainActivity.class);
                startActivity(i);
            } else {
                Toast.makeText(this, "User credentials are incorrect!", Toast.LENGTH_SHORT).show();
            }
        });
    }
}