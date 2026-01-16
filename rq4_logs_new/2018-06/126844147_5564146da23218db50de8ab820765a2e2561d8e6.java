package com.example.a.myapplication;

import android.os.Handler;
import android.support.v7.app.AppCompatActivity;
import android.os.Bundle;
import android.widget.TextView;

import com.example.a.myapplication.OBD.ObdConfigration.ObdConfig;
import com.example.a.myapplication.OBD.ObdData.obdLiveData;
import com.example.a.myapplication.OBD.obdApi.Commands.SpeedCommand;
import com.example.a.myapplication.OBD.obdApi.Commands.engine.RPMCommand;
import com.example.a.myapplication.OBD.obdApi.Commands.fuel.FuelLevelCommand;
import com.example.a.myapplication.OBD.obdApi.Commands.temperature.EngineCoolantTemperatureCommand;
import com.example.a.myapplication.OBD.obdApi.ObdCommand;

import java.util.ArrayList;
import java.util.LinkedList;

public class DriverModeActivity extends AppCompatActivity {

    private obdLiveData mobObdLiveData = new obdLiveData();
    private TextView[] t = new TextView[9];

    /**
     * related to the thread updateing the ui
     **/
    private final static int start = 1;
    private final static int stop = 0;
    private int state;

    private final int loopFristNumber = 0;
    private final int loopLastNumber = 3;



    @Override
    protected void onCreate(Bundle savedInstanceState) {
        super.onCreate(savedInstanceState);
        setContentView(R.layout.activity_driver_mode);


        t[0] = (TextView)findViewById(R.id.Text1);
        t[1] = (TextView)findViewById(R.id.Text2);
        t[2] = (TextView)findViewById(R.id.Text3);
        t[3] = (TextView)findViewById(R.id.Text4);


        state = start;

        // send the Fuel queue to the shared memory so worker thread run only desierd queue

        ArrayList<ObdCommand> cmds = ObdConfig.getCommands();

        ArrayList<ObdCommand> X  = new ArrayList<>();

        X.add(new SpeedCommand());
        X.add(new RPMCommand());
        X.add(new EngineCoolantTemperatureCommand());
        X.add(new FuelLevelCommand());


        mobObdLiveData.setQueuCommands(X);

        mobObdLiveData.setDataPlace(loopFristNumber,loopLastNumber);



        start();

    }

    @Override
    protected void onPause() {
        state = stop;
        super.onPause();

    }


    @Override
    protected void onResume() {
        super.onResume();
        state =start;
    }



    public void start() {

        final Handler h = new Handler();
        Runnable r = new Runnable() {
            @Override
            public void run() {
                while (state == 1) {
                    LinkedList<String> l = new LinkedList<String>(mobObdLiveData.getData());
                    String s = "";
                    for (int i = loopFristNumber; i <= loopLastNumber; i++) {
                        s += l.get(i);
                        s += "\n";
                    }
                    final String [] finalS =s.split("\n");
                    h.post(new Runnable() {
                        @Override
                        public void run() {

                            for(int i = 0;i<finalS.length;i++)
                            {
                                t[i].setText(finalS[i]);

                            }
                        }
                    });

                    try {
                        Thread.sleep(400);
                    } catch (InterruptedException e) {
                        e.printStackTrace();
                    }


                }

            }
        };
        Thread t = new Thread(r);
        t.start();
    }


}