package hu.oe.nik.szfmv.automatedcar.systemcomponents;

import hu.oe.nik.szfmv.automatedcar.model.WorldObject;
import hu.oe.nik.szfmv.automatedcar.virtualfunctionbus.VirtualFunctionBus;

import java.awt.*;
import java.util.List;

public class EmergencyBrake extends SystemComponent {
    Shape emergencyDistance;
    Shape warningDistance;

    public EmergencyBrake(VirtualFunctionBus virtualFunctionBus) {
        super(virtualFunctionBus);
    }

    @Override
    public void loop() {
        drawEmergencyDistances();
        checkEmergency();
    }

    private void drawEmergencyDistances(){
        //TODO
        calculateEmergencyDistance();
        calculateWarningDistance();
    }

    private int calculateEmergencyDistance(){
        //TODO
        return 0;
    }

    private int calculateWarningDistance(){
        //TODO
        return 0;
    }

    private void checkEmergency(){
        //TODO
        checkIntersection();
        warnDriver();
        applyBrake();
    }

    private boolean checkIntersection(){
        //TODO
        getObjectsFromRadarSensor();
        filterObjects();
        return false;
    }

    private List<WorldObject> getObjectsFromRadarSensor() {
        //TODO
        return null;
    }

    private List<WorldObject> filterObjects() {
        //TODO
        return null;
    }

    private void warnDriver(){
        //TODO
    }

    private void applyBrake(){
        //TODO
    }
}