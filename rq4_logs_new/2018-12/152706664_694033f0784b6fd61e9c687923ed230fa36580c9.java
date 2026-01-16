package mobile.project.cekjadwaldokter.Akun;

import android.content.Intent;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.view.View;

import mobile.project.cekjadwaldokter.MenuUtama.Drawer;
import mobile.project.cekjadwaldokter.R;

public class LoginActivity extends AppCompatActivity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_login);
    }

    public void Masuk(View view) {
        Intent intent = new Intent(LoginActivity.this, Drawer.class);
        startActivity(intent);
        finish();
    }
}