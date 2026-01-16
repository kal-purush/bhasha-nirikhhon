package hcmute.edu.vn.myfoody;

import androidx.appcompat.app.AppCompatActivity;

import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.ImageView;

public class ForgotPassword1 extends AppCompatActivity {

    ImageView imgBackForgotPassword1;
    Button btnChangePassword;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        getSupportActionBar().hide();
        setContentView(R.layout.activity_forgot_password1);

        imgBackForgotPassword1 = (ImageView) findViewById(R.id.imgBackForgotPassword);
        btnChangePassword = (Button) findViewById(R.id.buttonChangePassword);

        imgBackForgotPassword1.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                finish();
            }
        });

        btnChangePassword.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                Intent intent = new Intent(ForgotPassword1.this, ForgotPassword2.class);
                startActivity(intent);
            }
        });
    }
}