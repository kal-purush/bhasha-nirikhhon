import command.*;
import device.GarageDoor;
import device.Light;
import device.RemoteControl;

import java.io.File;

public class Runner {

    public static RemoteControl remote = new RemoteControl();

    public static void main(String[] args) {
        remote.setCommand(new LightOnCommand(new Light()));
        remote.buttonPressed();
        remote.setCommand(new GarageDoorOpenCommand(new GarageDoor()));
        remote.setCommand(new GarageDoorOpenCommand(new GarageDoor()));
        remote.buttonPressed();
        remote.buttonPressed();
    }
}