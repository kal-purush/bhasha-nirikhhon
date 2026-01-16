package sg.edu.nus.iss.vmcs;

/*
 * Copyright 2003 ISS.
 * The contents contained in this document may not be reproduced in any
 * form or by any means, without the written permission of ISS, other
 * than for the purpose for which it has been supplied.
 *
 */

import sg.edu.nus.iss.vmcs.system.*;
import sg.edu.nus.iss.vmcs.util.*;

/**
 *
 *
 * @version 3.0 5/07/2003
 * @author Olivo Miotto, Pang Ping Li
 */

public class Vmcs {

    private String propertiesFile;

    private Vmcs(String propertiesFile) {
        this.propertiesFile = propertiesFile;
    }

    public void start() {
        MainController mc = new MainController(propertiesFile);
        try {
            mc.start();
        } catch (VMCSException e) {
            System.out.println(
                "Vmcs.start: Error in system initialization: "
                    + e.getMessage());
            System.exit(0);
        }
    }

    public static void main(String args[]) {
        Vmcs vmcs = new Vmcs(args[0]);
        vmcs.start();
    }
}