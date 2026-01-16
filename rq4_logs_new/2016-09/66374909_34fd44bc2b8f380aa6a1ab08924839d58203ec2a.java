package toolbox.modems;

import toolbox.modems.Modem;

/**
 * Created by perrythomson on 8/23/16.
 */
public class CableModem extends Modem {
    String method = "cable connection";

    public void connect() {
        System.out.println("Connecting to the Internet ...");
        System.out.println("Using a " + method);
    }
}