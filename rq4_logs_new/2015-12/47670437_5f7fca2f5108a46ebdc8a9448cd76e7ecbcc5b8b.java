package com.app.bestbet.bestbet;

import android.content.Intent;
import android.content.SharedPreferences;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.view.Menu;
import android.view.MenuItem;
import android.view.View;
import android.widget.ArrayAdapter;
import android.widget.Button;
import android.widget.EditText;
import android.widget.Spinner;
import android.widget.Toast;

import java.util.ArrayList;

public class PastBetDetails extends AppCompatActivity implements View.OnClickListener{

    private SharedPreferences savedValues;
    private String betId;
    private BestBetDB db;
    private Bet currentBet;

    private Button mainMenuButton;

    private Spinner nameSpinner;

    private EditText betDescription;
    private EditText betAmount;
    private EditText betDate;

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_past_bet_details);

        db = new BestBetDB(this);

        savedValues = getSharedPreferences("SavedValues", MODE_PRIVATE);
        betId = savedValues.getString("betID", "");
        currentBet = db.getBet(betId);

        betDescription = (EditText) findViewById(R.id.txtBetDescription);
        betAmount = (EditText) findViewById(R.id.txtBetAmount);
        betDate = (EditText) findViewById(R.id.txtBetDate);

        betDescription.setText(currentBet.getDescription());
        betAmount.setText(String.valueOf(currentBet.getAmount()));
        betDate.setText(String.valueOf(currentBet.getDate()));

        mainMenuButton = (Button) findViewById(R.id.btnMainMenu);
        mainMenuButton.setOnClickListener(this);

        ArrayList<String> names = new ArrayList<String>();
        String currentPerson = "";

        for (Person person: db.getPeople()  ) {
            names.add(person.getName());
            if (currentBet.getPersonId() == person.getId())
            {
                currentPerson = person.getName();
            }
        }

        nameSpinner = (Spinner) findViewById(R.id.personSpinner);
        ArrayAdapter<String> spinnerAdapter = new ArrayAdapter<String>(this, android.R.layout.simple_spinner_item, names);
        nameSpinner.setAdapter(spinnerAdapter);
        nameSpinner.setSelection(names.indexOf(currentPerson));
        nameSpinner.setEnabled(false);
    }

    @Override
    public void onClick(View v) {
        Bet newBet = db.getBet(betId);
        switch (v.getId()) {
            case R.id.btnMainMenu:
                startActivity(new Intent(getApplicationContext(), MainMenu.class));
                break;
        }
    }


    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.menu_past_bet_details, menu);
        return true;
    }

    @Override
    public boolean onOptionsItemSelected(MenuItem item) {
        // Handle action bar item clicks here. The action bar will
        // automatically handle clicks on the Home/Up button, so long
        // as you specify a parent activity in AndroidManifest.xml.
        int id = item.getItemId();

        //noinspection SimplifiableIfStatement
        if (id == R.id.action_settings) {
            return true;
        }

        return super.onOptionsItemSelected(item);
    }
}