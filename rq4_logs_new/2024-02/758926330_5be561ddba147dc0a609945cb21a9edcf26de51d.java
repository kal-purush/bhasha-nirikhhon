package com.example.activitykayrenen;

import androidx.appcompat.app.AppCompatActivity;
import androidx.cardview.widget.CardView;

import android.app.Dialog;
import android.os.Bundle;
import android.view.View;
import android.view.ViewGroup;
import android.widget.Button;
import android.widget.Toast;

public class MainActivity extends AppCompatActivity {

    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_main);

        executeLogicDamingAlamHays();
    }

    public void executeLogicDamingAlamHays(){
        CardView firstCard = findViewById(R.id.firstCard);
        CardView secondCard = findViewById(R.id.secondCard);
        CardView thirdCard = findViewById(R.id.thirdCard);

        showFirstDialog(firstCard);
        showSecondDialog(secondCard);
        showThirdDialog(thirdCard);
    }
    public void showFirstDialog(CardView card){
        Dialog firstDialog = new Dialog(MainActivity.this);
        firstDialog.setContentView(R.layout.firstdialogbox);
        firstDialog.getWindow().setBackgroundDrawable(getDrawable(R.drawable.background));
        firstDialog.getWindow().setLayout(ViewGroup.LayoutParams.MATCH_PARENT,
                ViewGroup.LayoutParams.WRAP_CONTENT);

        card.setOnClickListener(new View.OnClickListener(){
            @Override
            public void onClick(View v){
                firstDialog.show();
            }
        });
        Button firstButton = firstDialog.findViewById(R.id.firstDialogButton);
        firstButton.setOnClickListener(new View.OnClickListener(){
            @Override
            public void onClick(View v){
                firstDialog.dismiss();
            }
        });
    }

    public void showSecondDialog(CardView card){
        Dialog secondDialog = new Dialog(MainActivity.this);
        secondDialog.setContentView(R.layout.seconddialogbox);
        secondDialog.getWindow().setBackgroundDrawable(getDrawable(R.drawable.background2));
        secondDialog.getWindow().setLayout(ViewGroup.LayoutParams.MATCH_PARENT,
                ViewGroup.LayoutParams.WRAP_CONTENT);

        card.setOnClickListener(new View.OnClickListener(){
            @Override
            public void onClick(View v){
                secondDialog.show();
            }
        });

        Button secondButton = secondDialog.findViewById(R.id.secondDialogButton);
        secondButton.setOnClickListener(new View.OnClickListener(){
            @Override
            public void onClick(View v){
                secondDialog.dismiss();
            }
        });
    }

    public void  showThirdDialog (CardView card){
        Dialog thirdDialog = new Dialog(MainActivity.this);
        thirdDialog.setContentView(R.layout.thirddialogbox);
        thirdDialog.getWindow().setBackgroundDrawable(getDrawable(R.drawable.background3));
        thirdDialog.getWindow().setLayout(ViewGroup.LayoutParams.MATCH_PARENT,
                ViewGroup.LayoutParams.WRAP_CONTENT);

        card.setOnClickListener(new View.OnClickListener(){
            @Override
            public void onClick(View v){
                thirdDialog.show();
            }
        });

        Button thirdButton = thirdDialog.findViewById(R.id.thirdDialogButton);
        thirdButton.setOnClickListener(new View.OnClickListener(){
            @Override
            public void onClick(View v){
                thirdDialog.dismiss();
            }
        });
    }

}