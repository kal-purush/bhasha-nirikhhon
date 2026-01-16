package me.spencer.phc.core.controllers;

/**
 * Created by Spencer on 4/22/2016.
 */
public class Controllers {

    private static final InterfaceController interfaceController = new InterfaceController();
    private static final MotorController motorController = new MotorController();

    public static InterfaceController getInterfaceController() {
        return interfaceController;
    }

    public static MotorController getMotorController() {
        return motorController;
    }
}