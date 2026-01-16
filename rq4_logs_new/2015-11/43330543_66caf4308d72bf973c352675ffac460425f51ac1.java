package com.leechangu.sweettask;

import android.app.Activity;
import android.app.AlertDialog;
import android.app.ProgressDialog;
import android.content.DialogInterface;
import android.os.Bundle;
import android.util.Log;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.ProgressBar;
import android.widget.Toast;

import com.parse.ParseUser;

/**
 * Created by CharlesGao on 15-11-25.
 */
public class BoundActivity extends Activity {

    private EditText editText;
    private Button button_find;
    private String partnerUsername;
    private AlertDialog alertDialog;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_bound);

        init();
        button_find.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                partnerUsername = editText.getText().toString();
                if (partnerUsername.equals("")){
                    Toast.makeText(BoundActivity.this,
                            "Username should not be empty", Toast.LENGTH_LONG).show();
                } else {
                    checkBindingPass();
                    if (checkBindingPass()){
                        AlertDialog.Builder builder = new AlertDialog.Builder(BoundActivity.this);
                        builder.setMessage(R.string.find_partner_alertdialog)
                                .setPositiveButton(R.string.confirm, new DialogInterface.OnClickListener() {
                                    public void onClick(DialogInterface dialog, int id) {

                                        alertDialog.dismiss();

                                        ProgressDialog progressDialog = new ProgressDialog(BoundActivity.this);
                                        progressDialog.setMessage("Sending Invitation...");

                                        UserMngRepository.bindPartner(partnerUsername);
                                        Toast.makeText(BoundActivity.this,
                                                "Partner added", Toast.LENGTH_LONG).show();


                                        progressDialog.show();

                                        finish();
                                    }
                                })
                                .setNegativeButton(R.string.cancel, new DialogInterface.OnClickListener() {
                                    public void onClick(DialogInterface dialog, int id) {
                                        alertDialog.dismiss();
                                    }
                                });
                        alertDialog = builder.create();
                        alertDialog.show();
                    }

                }

            }
        });





    }

    public void init(){
        editText = (EditText) findViewById(R.id.et_enter_name_of_your_partner);
        button_find = (Button) findViewById(R.id.bt_find_partner);
    }

    public boolean checkBindingPass(){

        ProgressDialog progressDialog = new ProgressDialog(BoundActivity.this);
        progressDialog.setMessage(getResources().getString(R.string.waiting_for_the_server));

        // input current username
        if (ParseUser.getCurrentUser().getUsername().equals(partnerUsername)) {
            progressDialog.dismiss();
            Toast.makeText(BoundActivity.this,
                    "Come on this is your username", Toast.LENGTH_SHORT).show();
            return false;
        }

        // TODO: now current user can add partner more than one times,
        // TODO: it will send invite notification more then one time, which is not good.
        if(UserMngRepository.isPartnerUsernameValid(partnerUsername)){
            // check if this user have already has a partner
            if (UserMngRepository.isThisUserHavePartnerAlready(partnerUsername)){
                progressDialog.dismiss();
                Toast.makeText(BoundActivity.this,
                        "Unfortunately, this user has a partner already", Toast.LENGTH_SHORT).show();
                return false;
            } else return true; // partner username valid and does not has a partner
        }else{
            progressDialog.dismiss();
            // partner username is invalid
            Toast.makeText(BoundActivity.this,
                    "Username not found", Toast.LENGTH_SHORT).show();
            return false;
        }

    }


}