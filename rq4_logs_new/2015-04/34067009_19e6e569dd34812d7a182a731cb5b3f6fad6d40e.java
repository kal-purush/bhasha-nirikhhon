/*§
  ===========================================================================
  MacVerifier
  ===========================================================================
  Copyright (C) 2015 Gianluca Costa
  ===========================================================================
  Permission is hereby granted, free of charge, to any person obtaining a copy
  of this software and associated documentation files (the "Software"), to deal
  in the Software without restriction, including without limitation the rights
  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
  copies of the Software, and to permit persons to whom the Software is
  furnished to do so, subject to the following conditions:

  The above copyright notice and this permission notice shall be included in
  all copies or substantial portions of the Software.

  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
  THE SOFTWARE.
  ===========================================================================
*/

package info.gianlucacosta.macverifier;

import info.gianlucacosta.macverifier.mac.HMacSha256Service;
import info.gianlucacosta.macverifier.mac.MacResult;
import info.gianlucacosta.macverifier.mac.MacService;
import info.gianlucacosta.macverifier.password.request.DefaultPasswordRequestService;
import info.gianlucacosta.macverifier.password.request.PasswordRequestService;
import info.gianlucacosta.macverifier.password.validation.DefaultPasswordValidationService;
import info.gianlucacosta.macverifier.ui.ConsoleUserInterface;
import info.gianlucacosta.macverifier.ui.SystemUserInterface;
import info.gianlucacosta.macverifier.ui.UserInterface;

import java.io.*;

/**
 * The application's starting point.
 */
public class App {
    private final UserInterface userInterface;
    private final MacService macService;


    public App(UserInterface userInterface, MacService macService) {
        this.userInterface = userInterface;
        this.macService = macService;
    }


    public void run(File mainFile) {
        try {
            if (!mainFile.isFile()) {
                userInterface.printFatal("Inexisting main file");
            }

            File macFile = new File(mainFile.getPath() + ".mac");

            boolean macFileExists = macFile.isFile();

            PasswordRequestService passwordRequestService = new DefaultPasswordRequestService(
                    userInterface,
                    new DefaultPasswordValidationService(),
                    !macFileExists
            );


            if (macFileExists) {
                userInterface.println("The related MAC file exists; proceeding to verification");
                verifyMainFile(passwordRequestService, mainFile, macFile);
            } else {
                userInterface.println(
                        String.format("A MAC file named '%s' will be created", macFile.getName())
                );
                createMacFile(passwordRequestService, mainFile, macFile);
            }
        } catch (Exception ex) {
            userInterface.printFatal(ex);
        }
    }

    private void createMacFile(PasswordRequestService passwordRequestService, File mainFile, File macFile) throws IOException {
        String macPassword = passwordRequestService.requestPassword("Password: ");

        if (macPassword == null) {
            System.exit(1);
        }

        try (BufferedInputStream mainInputStream = new BufferedInputStream(new FileInputStream(mainFile))) {
            byte[] macSalt = macService.createSalt();

            MacResult mainMacResult = macService.computeMac(macPassword, macSalt, mainInputStream);

            try (ObjectOutputStream macOutputStream = new ObjectOutputStream(new FileOutputStream(macFile))) {
                macOutputStream.writeObject(mainMacResult);
            }

            userInterface.println("OK - MAC file created. You should transmit it along with the main file");
        }
    }

    private void verifyMainFile(PasswordRequestService passwordRequestService, File mainFile, File macFile) throws IOException {
        String macPassword = passwordRequestService.requestPassword("Password: ");

        if (macPassword == null) {
            System.exit(1);
        }

        try (BufferedInputStream mainInputStream = new BufferedInputStream(new FileInputStream(mainFile));
             ObjectInputStream macInputStream = new ObjectInputStream(new FileInputStream(macFile))) {

            try {
                MacResult storedMacResult;

                try {
                    storedMacResult = (MacResult) macInputStream.readObject();
                } catch (EOFException ex) {
                    throw new IOException("Invalid MAC file");
                }


                byte[] macSalt = storedMacResult.getSalt();

                MacResult mainMacResult = macService.computeMac(macPassword, macSalt, mainInputStream);

                if (mainMacResult.equals(storedMacResult)) {
                    userInterface.println("OK - The main file is authentic and integrity checked");
                } else {
                    userInterface.printFatal("The main file (or its MAC file) has been tampered with! Or did you type the wrong password?");
                }
            } catch (ClassNotFoundException ex) {
                throw new RuntimeException(ex);
            }
        }
    }

    public static void main(String[] args) {
        UserInterface userInterface;

        Console systemConsole = System.console();
        if (systemConsole != null) {
            userInterface = new ConsoleUserInterface(systemConsole);
        } else {
            userInterface = new SystemUserInterface();
        }

        if (args.length < 1) {
            userInterface.printFatal("Usage: <main file path> [<MAC service FQN>]");
        }

        File mainFile = new File(args[0]);

        String macServiceClassName;
        if (args.length >= 2) {
            macServiceClassName = args[1];
        } else {
            macServiceClassName = HMacSha256Service.class.getName();
        }

        try {
            Class<? extends MacService> macServiceClass = (Class<? extends MacService>) Class.forName(macServiceClassName);
            MacService macService = macServiceClass.newInstance();

            App app = new App(userInterface, macService);
            app.run(mainFile);
        } catch (ClassNotFoundException | IllegalAccessException | InstantiationException ex) {
            userInterface.printFatal(ex);
        }
    }
}