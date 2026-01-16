package com.example.gorent.controle;

import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.text.TextUtils;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.Toast;

import com.example.gorent.R;
import com.example.gorent.dados.FireControl;
import com.example.gorent.entidade.Client;


public class AddClient extends AppCompatActivity implements View.OnClickListener  {

    private EditText nameEditText, adressEditText, idEditText;
    private Button addClient;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_add_client);

        setTitle("Adicionar Cliente");

        //initializing views
        nameEditText = (EditText) findViewById(R.id.name);
        adressEditText = (EditText) findViewById(R.id.adress);
        idEditText = (EditText) findViewById(R.id.id);

        addClient = (Button) findViewById(R.id.addClient);

        //attaching click listener
        addClient.setOnClickListener(this);

    }

    @Override
    public void onClick(View view) {
        if(view == addClient) {
            createClient();
        }
    }

    //method for user login
    private void createClient(){
        String name = nameEditText.getText().toString().trim();
        String adress  = adressEditText.getText().toString().trim();
        String id  = idEditText.getText().toString().trim();


        //checking if email and passwords are empty
        if(TextUtils.isEmpty(name)){
            Toast.makeText(this,"Por favor, digite um nome",Toast.LENGTH_LONG).show();
            return;
        }

        if(TextUtils.isEmpty(adress)){
            Toast.makeText(this,"Por favor, digite um endereco",Toast.LENGTH_LONG).show();
            return;
        }

        if(TextUtils.isEmpty(id)){
            Toast.makeText(this,"Por favor, digite o seu CPF",Toast.LENGTH_LONG).show();
            return;
        }

        Client client = new Client();

        client.name = name;
        client.adress = adress;
        client.id = id;

        new FireControl().writeNewClient(client);
    }
}