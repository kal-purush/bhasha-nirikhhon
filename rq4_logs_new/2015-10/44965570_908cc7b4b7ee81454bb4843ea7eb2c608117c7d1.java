package com.teamhrcontrol.teamhrcontrol;

import android.app.Service;
import android.content.Intent;
import android.os.IBinder;

import com.wahoofitness.connector.HardwareConnector;
import com.wahoofitness.connector.HardwareConnectorEnums;
import com.wahoofitness.connector.HardwareConnectorTypes;
import com.wahoofitness.connector.conn.connections.SensorConnection;

public class TeamHR extends Service {
    private HardwareConnector mHardwareConnector;
    private final HardwareConnector.Callback mHardwareConnectorCallback = new HardwareConnector.Callback(){
        @Override
        public void connectorStateChanged(HardwareConnectorTypes.NetworkType networkType, HardwareConnectorEnums.HardwareConnectorState hardwareConnectorState) {

        }

        @Override
        public void connectedSensor(SensorConnection sensorConnection) {

        }

        @Override
        public void disconnectedSensor(SensorConnection sensorConnection) {

        }

        @Override
        public void hasData() {

        }

        @Override
        public void onFirmwareUpdateRequired(SensorConnection sensorConnection, String s, String s1) {

        }
    };

    public TeamHR() {
    }

    @Override
    public void onCreate(){
        super.onCreate();
        mHardwareConnector = new HardwareConnector(this, mHardwareConnectorCallback);
    }

    @Override
    public void onDestroy(){
        super.onDestroy();
        mHardwareConnector.shutdown();
    }

    @Override
    public IBinder onBind(Intent intent) {
        // TODO: Return the communication channel to the service.
        throw new UnsupportedOperationException("Not yet implemented");
    }
}