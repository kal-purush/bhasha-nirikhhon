package br.udesc.weparty;

import androidx.appcompat.app.AppCompatActivity;

import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;

public class LoginRegistroActivity extends AppCompatActivity implements View.OnClickListener {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_login_registro);

        Button logar = (Button) findViewById(R.id.btnLogarAbrir);
        logar.setOnClickListener(this);

        Button registrar = (Button) findViewById(R.id.btnRegistrarAbrir);
        registrar.setOnClickListener(this);
    }

    @Override
    public void onClick(View v) {
        if (v.getId() == R.id.btnRegistrarAbrir) {
            Intent registrar = new Intent(v.getContext(), RegistroActivity.class);
            startActivity(registrar);
        }
        else if (v.getId() == R.id.btnLogarAbrir) {
            Intent logar = new Intent(v.getContext(), LoginActivity.class);
            startActivity(logar);
        }
    }

}