package sg.edu.np.mad.mad_assg;

import android.content.DialogInterface;
import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;

import androidx.appcompat.app.AlertDialog;
import androidx.appcompat.app.AppCompatActivity;

import com.google.android.material.textfield.TextInputLayout;

public class ForgotPassword extends AppCompatActivity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.forgot_password);

        Button cancel = (Button) findViewById(R.id.cancel);
        Button confirm = (Button) findViewById(R.id.confirm);

        TextInputLayout etUsername = (TextInputLayout) findViewById(R.id.forgot_username);
        TextInputLayout etemail = (TextInputLayout) findViewById(R.id.forgot_email);

        cancel.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                Intent intent = new Intent(ForgotPassword.this, StartPage.class);
                startActivity(intent);
            }
        });

        MyDBHandler dbHandler = new MyDBHandler(this,"User.db",null,1);

        confirm.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {

                String username = etUsername.getEditText().getText().toString();
                String email = etemail.getEditText().getText().toString();

                Boolean checkuser = dbHandler.user_checkUsername(username);
                Boolean checkuseremail = dbHandler.user_checkEmail(email);

                if ((checkuser && checkuseremail)){
                    Intent intent = new Intent(ForgotPassword.this, ResetPassword.class);
                    intent.putExtra("username", username);
                    startActivity(intent);
                }
                else{
                    AlertDialog.Builder builder = new AlertDialog.Builder(getApplicationContext());
                    new AlertDialog.Builder(getApplicationContext())
                            .setTitle("ERROR")
                            .setMessage("Username & Email not registered!!")
                            .setPositiveButton("Okay", new DialogInterface.OnClickListener() {
                                @Override
                                public void onClick(DialogInterface dialogInterface, int i) {
                                    Intent intent = new Intent(ForgotPassword.this, StartPage.class);
                                    startActivity(intent);
                                }
                            }).show();
                }
            }
        });

    }
}