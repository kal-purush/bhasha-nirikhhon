package com.kriscfoster.abstractFactory;

public class Main {

    public static Application configure(String os) {
        Application application = null;
        GUIFactory factory;

        if (os.equals("windows")) {
            factory = new WindowsFactory();
            application = new Application(factory);
        } else if (os.equals("macOS")) {
            factory = new MacOSFactory();
            application = new Application(factory);
        }

        return application;
    }

    public static void main(String[] args) {
        Application application = configure("windows");
        application.paint();
    }
}