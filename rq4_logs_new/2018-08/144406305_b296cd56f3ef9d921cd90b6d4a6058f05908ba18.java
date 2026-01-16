package com.cryptojobboard.cryptojobboard;

import android.content.Intent;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.view.View;
import android.widget.Toast;

public class candidateSpecificationPage extends AppCompatActivity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_candidate_specification_page);
        setTitle("Almost done!");


    }

    public void onSubmitClick( View view )
    {
        Toast.makeText(this, "On to next branch; Build my Intent!", Toast.LENGTH_SHORT).show();
        //Intent onToNext = new Intent(this.getApplicationContext(), XYZ);
        //startActivity( onToNext );
    }
}