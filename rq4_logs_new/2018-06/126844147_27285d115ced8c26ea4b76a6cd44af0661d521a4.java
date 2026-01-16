package com.example.a.myapplication;

import android.content.Intent;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.util.Log;
import android.view.View;
import android.widget.AdapterView;
import android.widget.ArrayAdapter;
import android.widget.ListView;

import java.util.ArrayList;
import java.util.List;

public class ConnectionRequestsActivity extends AppCompatActivity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_connection_requests);

        ListView lv = (ListView)this.findViewById(R.id.connectionRequests_ListView);
        ArrayList<String> connectionRequestsList = new ArrayList<>() ;
        ArrayAdapter<String> adapter = new ArrayAdapter<String>(getApplicationContext(),R.layout.connection_request_item,R.id.sender_username_tv,connectionRequestsList);
        lv.setAdapter(adapter);


        //TODO: get friend requests from database



        lv.setOnItemClickListener(new AdapterView.OnItemClickListener()
        {
            @Override
            public void onItemClick(AdapterView<?> adapterView, View view,int i,long l)
            {
                Intent intent= new Intent(getApplicationContext(),ConnectionSenderProfileActivity.class);
//                intent.putExtra("candy_name",candies[i].name);
//                intent.putExtra("candy_image", candies[i].image);
//                intent.putExtra("candy_price",candies[i].price);
//                intent.putExtra("candy_desc",candies[i].description);
//                Log.d("MainActivity","candy image: "+candies[i].image);
                startActivity(intent);
            }
        });
    }


}