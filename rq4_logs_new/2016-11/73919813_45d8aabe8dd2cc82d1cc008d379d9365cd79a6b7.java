package com.example.pavel.newproject;

import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.view.View;
import android.widget.Button;
import android.widget.TextView;

import java.util.ArrayList;
import java.util.Collections;
import java.util.Random;

public class MainActivity extends AppCompatActivity {

    Random rnd = new Random();
    int[] cards = mixedArray(16);
    Button b1, b2;
    int countClick = 1;
    int countStep  = 0;
    @Override
    protected void onCreate(Bundle savedInstanceState)
    {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);
    }

    public int[] mixedArray(int sizePlayingField)
    {
        int endIndexArrayInt  = sizePlayingField;
        int endIndexArrayList = (sizePlayingField / 2) + 1;

        int randomdArrayInt[] = new int[sizePlayingField];
        ArrayList<Integer> randomdArrayList = new ArrayList();

        for(int i = 1; i < endIndexArrayList; i++)
        {
            randomdArrayList.add(i);
            randomdArrayList.add(i);
        }

        Collections.shuffle(randomdArrayList);

        for(int i = 0; i < endIndexArrayInt; i++)
        {
            randomdArrayInt[i] = randomdArrayList.get(i);
        }

        return randomdArrayInt;
    }

    public void onClick(View v)
    {
        TextView textCountStep = (TextView)findViewById(R.id.Step);
        Button button = (Button) v;
        String tag = button.getTag().toString();
        int index = Integer.parseInt(tag);

        if(countClick == 1)
        {
            button.setText(Integer.toString(cards[index]));
            b1 = button;
            countClick = 2;
            countStep++;
        }
        else if(countClick == 2)
        {
            button.setText(Integer.toString(cards[index]));
            b2 = button;

            if(b1.getId() == b2.getId())
            {
                b1.setText("");
                b2.setText("");
                countClick = 1;
            }
            else if(b1.getText().equals(b2.getText()))
            {
                b1.setVisibility(View.INVISIBLE);
                b2.setVisibility(View.INVISIBLE);
                b1.setText("");
                b2.setText("");
                countClick = 1;
            }
            else
            {
                countClick = 3;
            }

            countStep++;
        }
        else if(countClick == 3)
        {
            b1.setText("");
            b2.setText("");
            countClick = 1;
        }

        textCountStep.setText(Integer.toString(countStep));
    }

}