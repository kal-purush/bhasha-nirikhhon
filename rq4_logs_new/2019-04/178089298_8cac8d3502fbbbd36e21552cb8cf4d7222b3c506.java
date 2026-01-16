package com.fiuba.tdpii.correapp.activities;

import android.app.Dialog;
import android.app.DialogFragment;
import android.content.Intent;
import android.os.Parcelable;
import android.support.design.widget.TextInputEditText;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.support.v7.widget.AppCompatEditText;
import android.text.Editable;
import android.text.TextWatcher;
import android.view.KeyEvent;
import android.view.LayoutInflater;
import android.view.View;
import android.view.ViewGroup;
import android.view.inputmethod.EditorInfo;
import android.widget.Button;
import android.widget.EditText;
import android.widget.Spinner;
import android.widget.TextView;
import android.widget.Toast;

import com.fiuba.tdpii.correapp.R;
import com.fiuba.tdpii.correapp.models.local.Pet;

public class AddPetActivity  extends AppCompatActivity {
//
//    @Override
//    protected void onCreate(Bundle savedInstanceState) {
//        super.onCreate(savedInstanceState);
//        setContentView(R.layout.activity_add_pet);
//    }

    private EditText nombre;

    private String nombreStr;


    private Spinner tipo;
    private String tipoStr;

    private Spinner size;
    private String sizeStr;

    private EditText obs;
    private String obsStr;

    private Button confirmBtn;
    private Bundle bundle;
    Pet pet1 ;

    Pet pet2 ;

    Pet pet3;


    @Override
    public void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);

        setContentView(R.layout.activity_add_pet);

        nombre = findViewById(R.id.nombre);


        tipo = findViewById(R.id.tipo_de_mascota);
        size = findViewById(R.id.tamaño_de_mascota);

        obs = findViewById(R.id.obs);

        confirmBtn = findViewById(R.id.confirm);
        
        bundle = getIntent().getParcelableExtra("bundle");

        pet1 = bundle.getParcelable("pet_1");

        pet2 = bundle.getParcelable("pet_2");

        pet3 = bundle.getParcelable("pet_3");


        confirmBtn.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {

                if(pet1 == null){
                    pet1 = new Pet();
                    pet1.nombre = nombre.getText().toString();
                    pet1.obs = obs.getText().toString();

                    pet1.tipo = tipo.getSelectedItem().toString();
                    pet1.size = tipo.getSelectedItem().toString();

                } else {
                    if (pet2 == null){

                        pet2 = new Pet();
                        pet2.nombre = nombre.getText().toString();
                        pet2.obs = obs.getText().toString();
                        pet2.tipo = tipo.getSelectedItem().toString();
                        pet2.size = tipo.getSelectedItem().toString();


                    } else{
                        if(pet3 == null){
                            pet3 = new Pet();
                            pet3.nombre = nombre.getText().toString();
                            pet3.obs = obs.getText().toString();
                            pet3.tipo = tipo.getSelectedItem().toString();
                            pet3.size = tipo.getSelectedItem().toString();

                        } else{
                            Toast.makeText(AddPetActivity.this, "No pueden viajar mas de 3 mascotas juntas" , Toast.LENGTH_LONG).show();
                        }
                    }
                }

                Intent navigationIntent = new Intent(AddPetActivity.this, CreateTripActivity.class);

                bundle.putParcelable("pet_1", pet1 );
                bundle.putParcelable("pet_2", pet2 );
                bundle.putParcelable("pet_3", pet3 );


                navigationIntent.putExtra("bundle", bundle );
                startActivity(navigationIntent);


                System.out.print(obs);
            }
        });
    }





}