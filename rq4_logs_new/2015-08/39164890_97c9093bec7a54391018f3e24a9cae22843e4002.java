package loginusingparsesdk;

import android.app.Activity;
import android.app.AlertDialog;
import android.content.Context;
import android.content.DialogInterface;
import android.content.Intent;
import android.os.Bundle;
import android.text.TextUtils;
import android.view.Menu;
import android.view.MenuItem;
import android.view.View;
import android.widget.Button;
import android.widget.EditText;
import android.widget.Toast;

import com.parse.LogInCallback;
import com.parse.Parse;
import com.parse.ParseAnalytics;
import com.parse.ParseException;
import com.parse.ParseObject;
import com.parse.ParseUser;
import com.parse.starter.R;

import java.util.Locale;

import trial.Convo;
import trial.Listing;
import trial.MainActivity;
import trial.Message;

public class Welcome extends Activity {
    Button btn_LoginIn = null;
    Button btn_SignUp = null;
    Boolean isInitialized = false;

    // flag for Internet connection status
    Boolean isInternetPresent = false;
    // Connection detector class
    ConnectionDetector cd;


    @Override
    protected void onCreate(Bundle savedInstanceState) {
        // TODO Auto-generated method stub
        super.onCreate(savedInstanceState);
        setContentView(R.layout.welcome_screen);
        //Initializing Parse SDK
        Intent in = getIntent();
        if (in != null){
            if (in.getExtras()!=null){
        isInitialized = in.getExtras().getBoolean("isInitialized");}}
        if (!isInitialized) {
        onCreateParse();}
        //Calling ParseAnalytics to see Analytics of our app
        ParseAnalytics.trackAppOpened(getIntent());

        // creating connection detector class instance
        cd = new ConnectionDetector(getApplicationContext());

        btn_LoginIn = (Button) findViewById(R.id.logIn);
        btn_SignUp = (Button) findViewById(R.id.makeAccount);

        btn_LoginIn.setOnClickListener(new View.OnClickListener() {

            @Override
            public void onClick(View v) {
                // TODO Auto-generated method stub
                Intent in = new Intent(Welcome.this, Login.class);
                in.putExtra("isInitialized", true);
                startActivity(in);

            }
        });

        btn_SignUp.setOnClickListener(new View.OnClickListener() {

            @Override
            public void onClick(View v) {
                // TODO Auto-generated method stub
                Intent in = new Intent(Welcome.this, SignUp.class);
                startActivity(in);
            }
        });
    }

    public void onCreateParse() {
        Parse.enableLocalDatastore(this);
        ParseObject.registerSubclass(Listing.class);
        ParseObject.registerSubclass(Message.class);
        ParseObject.registerSubclass(Convo.class);
        Parse.initialize(this, "OHN1vjNjDDN5L46ztns0EGB7ApSq1rXi3RrHksN8", "w1xlxox48cSzRWflCfuzR5Oa6gG5qU6qlKAhA03z");
    }
}
