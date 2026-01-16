package hcmute.edu.vn.myfoody;

import androidx.appcompat.app.AppCompatActivity;

import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.ImageButton;

public class ChangePasswordActivity extends AppCompatActivity {

    ImageButton imgBackChangePassword;
    Button btnXacNhan;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_change_password);

        imgBackChangePassword = (ImageButton) findViewById(R.id.imgBackChangePassword);
        btnXacNhan = (Button) findViewById(R.id.buttonXacNhan) ;

        imgBackChangePassword.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                finish();
            }
        });

        btnXacNhan.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                finish();
            }
        });
    }
}