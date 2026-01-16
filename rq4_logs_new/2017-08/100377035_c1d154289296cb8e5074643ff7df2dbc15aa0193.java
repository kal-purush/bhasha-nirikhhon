package com.teamtreehouse.ribbit.ui;

import android.app.Activity;
import android.content.Intent;
import android.net.Uri;
import android.os.Bundle;
import android.util.Log;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.Toast;

import com.teamtreehouse.ribbit.R;
import com.teamtreehouse.ribbit.models.User;

import java.util.ArrayList;

/*This class is called from main to handle the creation of the message
and sending the parameters of the message to an already installed mEmailEditText app on the devise
to mSend the message.
SMS API resource: https://www.tutorialspoint.com/android/android_sending_sms.htm*/

public class MessageActivity extends Activity {
    private static final String TAG = MessageActivity.class.getSimpleName();
    private static final int RECIPIENT_EMAIL = 7;
    EditText mEmailEditText;
    EditText mTextEditText;
    Button mSend;
    protected ArrayList<User> mFriends = new ArrayList<>();
    String mEmail;
    String mText;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        Log.d(TAG,"onCreate");
        setContentView(R.layout.activity_message);
//Inflate the message layout items.
        mEmailEditText = (EditText) findViewById(R.id.recipient_email);
        mTextEditText = (EditText) findViewById(R.id.text);
        mSend = (Button) findViewById(R.id.send_btn);

//Get the list of friends.
        mFriends = getIntent().getParcelableArrayListExtra("FRIEND_LIST");

//Get the recipient mEmailEditText and insert it in the mEmailEditText item of the layout.
        mEmailEditText.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                Intent intent = new Intent(MessageActivity.this, RecipientEmailActivity.class);
                intent.putExtra("FRIEND_LIST",mFriends);
                startActivityForResult(intent,RECIPIENT_EMAIL);
            }
        });

//Send the message when mEmailEditText and mTextEditText item in the layout are filled.
        mSend.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View view) {
                mText= mTextEditText.getText().toString();
                if(mEmail!=null) {
                    if(!mText.isEmpty()) {
                        sendMessage();
                        finish();
                    }else{
                        Toast.makeText(MessageActivity.this, R.string.text_body_empty, Toast.LENGTH_LONG).show();
                    }
                }else{
                    Toast.makeText(MessageActivity.this, R.string.email_body_empty, Toast.LENGTH_LONG).show();
                }
            }
        });

    }
/*Set the mEmailEditText and mTextEditText item of the layout.
Email item depends on the result of the  RecipientEmailActivity.*/
    @Override
    protected void onActivityResult(int requestCode, int resultCode, Intent data) {
        super.onActivityResult(requestCode, resultCode, data);
        if (resultCode == RESULT_OK){
            if(requestCode == RECIPIENT_EMAIL){
                mEmail=data.getStringExtra("RECIPIENT_EMAIL");
                mEmailEditText.setText(mEmail);
            }else{
                Toast.makeText(this,"Sorry the recipient mEmailEditText was not added.", Toast.LENGTH_SHORT).show();
            }
        }else{
            Toast.makeText(this,"Sorry the recipient mEmailEditText was not added.", Toast.LENGTH_SHORT).show();
        }
    }

//This method utilizes already installed mEmailEditText and messenger apps to mSend actual messages
    protected void sendMessage() {
        Toast.makeText(MessageActivity.this,"Select the mEmailEditText app to use to mSend your message", Toast.LENGTH_LONG).show();
        Intent emailIntent = new Intent(Intent.ACTION_SEND);
        emailIntent.setData(Uri.parse("mailto:"));
        emailIntent.setType("mTextEditText/plain");
        emailIntent.putExtra(Intent.EXTRA_EMAIL, new String [] {mEmail});
        emailIntent.putExtra(Intent.EXTRA_SUBJECT, "");
        emailIntent.putExtra(Intent.EXTRA_TEXT, mText);

        try {
            startActivity(Intent.createChooser(emailIntent, "Send mail..."));
            finish();
            Log.d(TAG,"Finished sending mEmailEditText...");
        } catch (android.content.ActivityNotFoundException ex) {
            Toast.makeText(MessageActivity.this, "There is no mEmailEditText client installed.", Toast.LENGTH_SHORT).show();
        }
    }
}


