package com.example.kevin.matrix;

import android.graphics.Color;
import android.os.Bundle;
import android.support.v7.app.AppCompatActivity;
import android.support.v7.widget.Toolbar;
import android.text.InputType;
import android.view.Menu;
import android.view.MenuItem;
import android.view.View;
import android.view.ViewGroup;
import android.widget.AdapterView;
import android.widget.ArrayAdapter;
import android.widget.EditText;
import android.widget.LinearLayout;
import android.widget.Spinner;

/**
 * Created by Kevin on 11/13/2016.
 */

public class MatrixInput extends AppCompatActivity {

    int numberOfRows = 0;
    int numberOfColumns = 0;
    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.matrix_input);
        Toolbar toolbar = (Toolbar) findViewById(R.id.toolbar);
        setSupportActionBar(toolbar);

        final LinearLayout matrix = (LinearLayout) findViewById(R.id.matrix);
        final LinearLayout spinners = (LinearLayout) findViewById(R.id.spinners);
        Spinner rowsSpinner = new Spinner(getApplicationContext());
        Spinner columnSpinner = new Spinner(getApplicationContext());

        ArrayAdapter<CharSequence> adapter = ArrayAdapter.createFromResource(this,
                R.array.numbers, android.R.layout.simple_spinner_item);
        adapter.setDropDownViewResource(android.R.layout.simple_spinner_dropdown_item);
        rowsSpinner.setAdapter(adapter);
        columnSpinner.setAdapter(adapter);
        spinners.addView(rowsSpinner);
        spinners.addView(columnSpinner);


        rowsSpinner.setOnItemSelectedListener(new AdapterView.OnItemSelectedListener() {
            @Override
            public void onItemSelected(AdapterView<?> adapterView, View view, int i, long l) {
                int oldNumber = numberOfRows;
                numberOfRows = Integer.parseInt((String) adapterView.getItemAtPosition(i));
                if (numberOfRows > oldNumber) {
                    for (int index = oldNumber; index < numberOfRows; index++) {
                        LinearLayout newRow = new LinearLayout(getApplicationContext());
                        newRow.setOrientation(LinearLayout.HORIZONTAL);
                        /*
                        ViewGroup.LayoutParams params = newRow.getLayoutParams();
                        params.height = ViewGroup.LayoutParams.WRAP_CONTENT;
                        params.width = ViewGroup.LayoutParams.WRAP_CONTENT;
                        newRow.setLayoutParams(params);
                        */

                        for (int index2 = 0; index2 < numberOfColumns; index2++) {
                            EditText et = new EditText(getApplicationContext());
                            et.setHint("Number");
                            et.setTextColor(Color.BLACK);
                            et.setHintTextColor(Color.BLACK);
                            et.setInputType(InputType.TYPE_CLASS_NUMBER);
                            newRow.addView(et);
                        }
                        matrix.addView(newRow);
                    }
                } else {
                    for (int index = oldNumber - 1; index >= numberOfRows; index--) {
                        matrix.removeViewAt(index);
                    }
                }
            }

            @Override
            public void onNothingSelected(AdapterView<?> adapterView) {

            }
        });
        columnSpinner.setOnItemSelectedListener(new AdapterView.OnItemSelectedListener() {
            @Override
            public void onItemSelected(AdapterView<?> adapterView, View view, int i, long l) {
                int oldNumber = numberOfColumns;
                numberOfColumns = Integer.parseInt((String) adapterView.getItemAtPosition(i));
                if (numberOfColumns > oldNumber) {
                    for (int index = 0; index < numberOfRows; index++) {
                        LinearLayout current = (LinearLayout) matrix.getChildAt(index);
                        for (int index2 = oldNumber; index2 < numberOfColumns; index2++) {
                            EditText et = new EditText(getApplicationContext());
                            et.setHint("Number");
                            et.setTextColor(Color.BLACK);
                            et.setHintTextColor(Color.BLACK);
                            et.setInputType(InputType.TYPE_CLASS_NUMBER);
                            current.addView(et);
                        }

                    }
                } else {
                    for (int index = 0; index < numberOfRows; index++) {
                        LinearLayout current = (LinearLayout) matrix.getChildAt(index);
                        for (int index2 = oldNumber - 1; index2 >= numberOfColumns; index2--) {
                            current.removeViewAt(index2);
                        }
                    }
                }
            }

            @Override
            public void onNothingSelected(AdapterView<?> adapterView) {

            }
        });
    }

    @Override
    public boolean onCreateOptionsMenu(Menu menu) {
        // Inflate the menu; this adds items to the action bar if it is present.
        getMenuInflater().inflate(R.menu.menu_main, menu);
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