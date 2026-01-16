package com.example.chanti.blood;

import android.os.Bundle;
import android.support.v7.app.AppCompatActivity;
import android.util.Log;
import android.widget.EditText;
import android.widget.Spinner;

/**
 * Created by chanti on 27-Feb-16.
 */
public class home extends AppCompatActivity {
    String firstNameTxt, lastNameTxt, passwordTxt, phoneTxt, userNameTxt, addressTxt;
    String bloodGroupTxt;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.registration);

        firstNameTxt = ((EditText) findViewById(R.id.editFirstName)).getText().toString();
        lastNameTxt = ((EditText) findViewById(R.id.editLastName)).getText().toString();
        passwordTxt = ((EditText) findViewById(R.id.editPassword)).getText().toString();
        phoneTxt = ((EditText) findViewById(R.id.editPhoneNumber)).getText().toString();
        userNameTxt = ((EditText) findViewById(R.id.editUserName)).getText().toString();
        addressTxt = ((EditText) findViewById(R.id.address)).getText().toString();
        bloodGroupTxt = ((Spinner) findViewById(R.id.spinnerBloodGroup)).getSelectedItem().toString();

        Log.d("first", firstNameTxt);
        Log.d("last", lastNameTxt);
        Log.d("username", userNameTxt);
        Log.d("password", passwordTxt);
        Log.d("add", addressTxt);
        Log.d("blood",bloodGroupTxt);
    }

}