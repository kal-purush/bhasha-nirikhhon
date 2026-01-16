package com.example.a.myapplication;

import android.content.Context;
import android.content.Intent;
import android.os.Bundle;
import android.support.v7.app.AppCompatActivity;
import android.widget.TextView;

public class AcceptorInfo extends AppCompatActivity {

    ////////////////////////////////////
    /////// YOMNA HESHAM 19 APRIL //////
    ////////////////////////////////////
    private static final String TAG = "Acceptor Info";
    private static Context mContext;

    private static int acceptorID;
    private static String acceptorName;
    private static String acceptorPhone;
    private static int acceptorLng;
    private static int acceptorLat;
    private static int acceptorAtit;

    private TextView TextViewAcceptorName, TextViewAcceptorPhone,
                    TextViewAcceptorLng, TextViewAcceptorLat, TextViewAcceptorAtit;
    ////////////////////////////////////
    ////////////////////////////////////

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_acceptor_info);

        ////////////////////////////////////
        /////// YOMNA HESHAM 19 APRIL //////
        ////////////////////////////////////
        Intent i = getIntent();
        acceptorID    = i.getIntExtra(   "acceptorID", -1);
        acceptorName  = i.getStringExtra("username");
        acceptorPhone = i.getStringExtra("phone"   );
        acceptorLng   = i.getIntExtra(   "lng" , -1);
        acceptorLat   = i.getIntExtra(   "lat" , -1);
        acceptorAtit  = i.getIntExtra(   "atit", -1);

        TextViewAcceptorName  = (TextView) findViewById(R.id.name);
        TextViewAcceptorPhone = (TextView) findViewById(R.id.phone);
        TextViewAcceptorLng   = (TextView) findViewById(R.id.lng);
        TextViewAcceptorLat   = (TextView) findViewById(R.id.lat);
        TextViewAcceptorAtit  = (TextView) findViewById(R.id.atit);

        TextViewAcceptorName.setText (acceptorName );
        TextViewAcceptorPhone.setText(acceptorPhone);
        TextViewAcceptorLng.setText  (acceptorLng  );
        TextViewAcceptorLat.setText  (acceptorLat  );
        TextViewAcceptorAtit.setText (acceptorAtit );

        ////////////////////////////////////
        ////////////////////////////////////

    }
}