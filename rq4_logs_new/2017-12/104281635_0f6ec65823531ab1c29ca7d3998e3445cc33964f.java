package com.example.user.testezin;

import android.content.Intent;
import android.os.Bundle;
import android.support.v7.app.AppCompatActivity;
import android.view.View;
import android.widget.ImageButton;
import android.widget.ImageView;
import android.widget.TextView;
import android.widget.Toast;

/**
 * Created by Vinicius on 29/11/2017.
 */

public class Activity_admin_pagina_principal extends AppCompatActivity {

    String emailUser, nomeUser;
    private boolean admin;
    UsuarioDAO userDao;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.admin_principal);

        userDao = new UsuarioDAO(getApplicationContext());
        //Email passado como parâmetro da activity
        Intent intent = getIntent();
        emailUser = intent.getStringExtra("emailUser");
        admin = intent.getBooleanExtra("admin", false);
        if(userDao.getUsuarioEmail(emailUser) != null) {
            Usuario user = userDao.getUsuarioEmail(emailUser);
        } else {
            Toast.makeText(getApplicationContext(),"Usuario não encontrado.",Toast.LENGTH_SHORT).show();
        }


        ImageView btnConteudo = findViewById(R.id.btnConteudo);
        ImageButton btnVoltar = findViewById(R.id.btnVoltarInicioAdmin);


        btnConteudo.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                Intent intent= new Intent(Activity_admin_pagina_principal.this, MainActivity.class);
                intent.putExtra("emailUser", emailUser);
                intent.putExtra("admin", admin);
                startActivity(intent);
            }
        });

        btnVoltar.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                Intent intent = new Intent(Activity_admin_pagina_principal.this, Activity_inicio_pagina_inicial.class);
                intent.putExtra("emailUser", emailUser);
                intent.putExtra("admin", admin);
                startActivity(intent);
            }
        });
    }

}