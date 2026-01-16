package com.example.lauramarra.quintafeira;

import android.app.Activity;
import android.app.AlertDialog;
import android.content.DialogInterface;
import android.content.Intent;
import android.os.Bundle;
import android.view.View;
import android.widget.AdapterView;
import android.widget.ArrayAdapter;
import android.widget.Button;
import android.widget.ListView;
import android.widget.TextView;

import java.util.Calendar;

public class TesteAcesso2 extends Activity {

    MyDBHandler dbHandler;
    MyDBHandler2 dbChecker;
    TextView textView;
    Button getStatus;
    private ListView menuList;
    String[] menuOptions = new String[20];

    @Override
    public void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_teste_acesso2);

        textView = (TextView) findViewById(R.id.bdText2);
        //getStatus = (Button) findViewById(R.id.getStatus2);

        textView.setText("Olá! " + "\n" + "Estas são suas aulas do dia:");

        menuList = (ListView) findViewById(R.id.list);

        dbChecker = new MyDBHandler2(this, null, null, 1);
        dbHandler = new MyDBHandler(this, null, null, 1);

        menuOptions = dbChecker.getClassToday().split(";");

        ArrayAdapter<String> adapter =
                new ArrayAdapter<String>(this, android.R.layout.simple_list_item_1, menuOptions);

        menuList.setAdapter(adapter);

        menuList.setOnItemClickListener(new AdapterView.OnItemClickListener() {
            @Override
            public void onItemClick(AdapterView<?> parent, View view, int position, long id) {

                Calendar sCalendar = Calendar.getInstance();
                //String dayLongName = sCalendar.getDisplayName(Calendar.DAY_OF_WEEK, Calendar.LONG, Locale.getDefault());
                int day = sCalendar.get(Calendar.DAY_OF_WEEK);
                Intent i;
                //textView.setText(day);

                AlertDialog alertDialog;

                switch (position) {
                    case 0:

                        //String local = dbHandler.getLocal(menuOptions[0]);

                        alertDialog = new AlertDialog.Builder(TesteAcesso2.this).create();
                        alertDialog.setTitle(menuOptions[0]);
                        alertDialog.setMessage("Local: H217" + "\n" + "Status: Confimada!");
                        alertDialog.setButton("OK", new DialogInterface.OnClickListener() {
                            public void onClick(DialogInterface dialog, int which) {
                            }
                        });
                        alertDialog.show();
                        break;
                    case 1:
                        alertDialog = new AlertDialog.Builder(TesteAcesso2.this).create();
                        alertDialog.setTitle(menuOptions[1]);
                        alertDialog.setMessage("Local: F112" + "\n" + "Status: Cancelada!");
                        alertDialog.setButton("OK", new DialogInterface.OnClickListener() {
                            public void onClick(DialogInterface dialog, int which) {
                            }
                        });
                        alertDialog.show();
                        break;
                    case 2:
                        alertDialog = new AlertDialog.Builder(TesteAcesso2.this).create();
                        alertDialog.setTitle(menuOptions[2]);
                        alertDialog.setMessage("Local: H217" + "\n" + "Status: Confimada!");
                        alertDialog.setButton("OK", new DialogInterface.OnClickListener() {
                            public void onClick(DialogInterface dialog, int which) {
                            }
                        });
                        alertDialog.show();
                        break;
                }
            }

        });

       /* getStatus.setOnClickListener(new View.OnClickListener() {
            @Override
            public void onClick(View v) {
                //getID();
                //printDatabase();
            }
        });
        */
    }
    public void getID() {
        String dbString = dbChecker.getID();
        textView.setText(dbString);
    }

}