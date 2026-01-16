/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package midp;

import java.io.DataInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import javax.microedition.io.Connector;
import javax.microedition.io.file.FileConnection;
import javax.microedition.midlet.*;
import javax.microedition.lcdui.*;
import org.netbeans.microedition.lcdui.pda.FileBrowser;

/**
 * @author Ruholla
 */
public class Oughat_Sharei_Shia extends MIDlet implements CommandListener {

    private C100 secret = new C100();

    protected void destroyApp(boolean unconditional) throws MIDletStateChangeException {
        secret.destroyApp(unconditional);
    }

    protected void pauseApp() {
        secret.pauseApp();
    }

    protected void startApp() throws MIDletStateChangeException {
        secret.startApp();
    }

    public void commandAction(Command c, Displayable d) {
        secret.commandAction(c, d);
    }

    private class C100 implements CommandListener{//extends MIDlet implements CommandListener {
        //<editor-fold defaultstate="collapsed" desc=" Generated Fields ">

        private Command exitCommand;
        private Command helpCommand;
        private Command nextCommand;
        private Command goSettingsCommand;
        private Command cancelCommand;
        private Command oKCommand;
        private Command backCommand1;
        private Command okCommand1;
        private Command altitudeSettingsCommand;
        private Command backCommand2;
        private Command loadFileCommand;
        private Command backCommand3;
        private Command okCommand;
        private Command cancelCommand1;
        private Command backToAltitudeSettCommand;
        private Command lCommand2;
        private Command tryEgainBackCommand;
        private Command backCommand;
        private Command okCommand2;
        private Command loadFromFileItemCommand;
        private Command cancelCommand2;
        private Command tryAgainCancelCommand3;
        private Command cancelCommand3;
        private Command backCommand4;
        private Command backCommand5;
        private Command resetBackCommand6;
        private Command backToMainPageCommand7;
        private Command showLocationsItemCommand;
        private Command cancelCommand4;
        private Command searchOkCommand3;
        private Command globalBack;
        private Form MainPage;
        private StringItem stringItem;
        private Form altitudeSettings;
        private TextField textField1;
        private TextField textField2;
        private TextField textField;
        private TextField textField4;
        private TextField textField3;
        private TextField textField11;
        private StringItem stringItem9;
        private Form inputData;
        private TextField textField5;
        private TextField textField6;
        private TextField textField7;
        private TextField textField8;
        private Spacer spacer;
        private TextField textField9;
        private Spacer spacer1;
        private ChoiceGroup choiceGroup;
        private ChoiceGroup choiceGroup1;
        private List settingsList;
        private FileBrowser altitudeFileBrowser;
        private Alert alert;
        private Form locationSettings;
        private FileBrowser locationsFileBrowser;
        private Alert alert1;
        private Alert alert2;
        private Form showingResultForm;
        private StringItem stringItem3;
        private StringItem stringItem2;
        private StringItem stringItem7;
        private StringItem stringItem6;
        private StringItem stringItem5;
        private StringItem stringItem4;
        private StringItem stringItem8;
        private StringItem stringItem1;
        private Form inputFirstOfLocation;
        private TextField textField10;
        private TextBox helpPage0;
        private Form helpPage;
        private StringItem stringItem10;
        private Ticker ticker;
        //</editor-fold>
        private boolean f1 = false;
        private C20 f2;
        private C45 f3 = C12.getAltitudeCalculator();
        private boolean f4 = false;
        private C11 f5 = new C11();
        private boolean f6 = false;
        private boolean f7 = false;
        private C98 f8 = new C98();
        private CC f9 = new CC();
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Methods ">
        //</editor-fold>
        //<editor-fold defaultstate="collapsed" desc=" Generated Method: initialize ">
        /**
         * Initilizes the application.
         * It is called only once when the MIDlet is started. The method is called before the <code>startMIDlet</code> method.
         */
        private void initialize() {
            // write pre-initialize user code here
            // write post-initialize user code here
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Method: startMIDlet ">
        /**
         * Performs an action assigned to the Mobile Device - MIDlet Started point.
         */
        public void startMIDlet() {
            // write pre-action user code here

            switchDisplayable(null, getMainPage());
            // write post-action user code here
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Method: resumeMIDlet ">
        /**
         * Performs an action assigned to the Mobile Device - MIDlet Resumed point.
         */
        public void resumeMIDlet() {
            // write pre-action user code here
            switchDisplayable(null, getMainPage());
            // write post-action user code here
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Method: switchDisplayable ">
        /**
         * Switches a current displayable in a display. The <code>display</code> instance is taken from <code>getDisplay</code> method. This method is used by all actions in the design for switching displayable.
         * @param alert the Alert which is temporarily set to the display; if <code>null</code>, then <code>nextDisplayable</code> is set immediately
         * @param nextDisplayable the Displayable to be set
         */
        public void switchDisplayable(Alert alert, Displayable nextDisplayable) {
            // write pre-switch user code here
            Display display = getDisplay();
            if (alert == null) {
                display.setCurrent(nextDisplayable);
            } else {
                display.setCurrent(alert, nextDisplayable);
            }
            // write post-switch user code here
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Method: commandAction for Displayables ">
        /**
         * Called by a system to indicated that a command has been invoked on a particular displayable.
         * @param command the Command that was invoked
         * @param displayable the Displayable where the command was invoked
         */
        public void commandAction(Command command, Displayable displayable) {
            // write pre-action user code here
            if (displayable == MainPage) {
                if (command == exitCommand) {
                    // write pre-action user code here
                    exitMIDlet();
                    // write post-action user code here
                } else if (command == goSettingsCommand) {
                    // write pre-action user code here
                    switchDisplayable(null, getSettingsList());
                    // write post-action user code here
                } else if (command == helpCommand) {
                    // write pre-action user code here
                    switchDisplayable(null, getHelpPage());
                    // write post-action user code here
                    C99.x(0);
                } else if (command == nextCommand) {
                    // write pre-action user code here
                    switchDisplayable(null, getInputData());
                    // write post-action user code here
                }
            } else if (displayable == alert) {
                if (command == backToAltitudeSettCommand) {
                    // write pre-action user code here
                    switchDisplayable(null, getAltitudeSettings());
                    // write post-action user code here
                } else if (command == tryEgainBackCommand) {
                    // write pre-action user code here
                    switchDisplayable(null, getAltitudeFileBrowser());
                    // write post-action user code here
                }
            } else if (displayable == alert1) {
                if (command == backCommand4) {
                    // write pre-action user code here
                    switchDisplayable(null, getLocationSettings());
                    // write post-action user code here
                } else if (command == tryAgainCancelCommand3) {
                    // write pre-action user code here
                    switchDisplayable(null, getLocationsFileBrowser());
                    // write post-action user code here
                }
            } else if (displayable == alert2) {
                if (command == backCommand5) {
                    // write pre-action user code here
                    switchDisplayable(null, getInputData());
                    // write post-action user code here
                }
            } else if (displayable == altitudeFileBrowser) {
                if (command == FileBrowser.SELECT_FILE_COMMAND) {
                    // write pre-action user code here
                    // write post-action user code here
                } else if (command == cancelCommand1) {
                    // write pre-action user code here
                    switchDisplayable(null, getAltitudeSettings());
                    // write post-action user code here
                } else if (command == okCommand) {
                    // write pre-action user code here
                    try {
                        m2();
                    } catch (Throwable e) {
                        C17.m1(e);
                        this.f1 = true;
                    }
                    m1();
                    // write post-action user code here
                }
            } else if (displayable == altitudeSettings) {
                if (command == cancelCommand) {
                    // write pre-action user code here
                    switchDisplayable(null, getSettingsList());
                    // write post-action user code here
                } else if (command == loadFileCommand) {
                    // write pre-action user code here
                    switchDisplayable(null, getAltitudeFileBrowser());
                    // write post-action user code here
                } else if (command == oKCommand) {
                    // write pre-action user code here
                    String d = "h";
//                d=new String


                    m3();
                    switchDisplayable(null, getSettingsList());
                    // write post-action user code here
                }
            } else if (displayable == helpPage) {
                if (command == globalBack) {
                    // write pre-action user code here
                    switchDisplayable(null, getMainPage());
                    // write post-action user code here
                }
            } else if (displayable == inputData) {
                if (command == backCommand1) {
                    // write pre-action user code here
                    switchDisplayable(null, getMainPage());
                    // write post-action user code here
                } else if (command == okCommand1) {
                    // write pre-action user code here
                    try {
                        m8();
                        this.f7 = false;
                    } catch (Throwable t) {
                        C17.m1(t);
                        this.f7 = true;
                    }

                    method1();
                    // write post-action user code here
                } else if (command == showLocationsItemCommand) {
                    // write pre-action user code here
                    switchDisplayable(null, getInputFirstOfLocation());
                    // write post-action user code here

                }
            } else if (displayable == inputFirstOfLocation) {
                if (command == cancelCommand4) {
                    // write pre-action user code here
                    switchDisplayable(null, getInputData());
                    // write post-action user code here
                } else if (command == searchOkCommand3) {
                    // write pre-action user code here

                    getShowingLocationsList().update(this.f5.m2(),
                            this.f5.m4(getTextField10().getString()));
                    switchDisplayable(null, this.getShowingLocationsList());
                    // write post-action user code here

                }
            } else if (displayable == locationSettings) {
                if (command == backCommand) {
                    // write pre-action user code here
                    switchDisplayable(null, getSettingsList());
                    // write post-action user code here
                } else if (command == loadFromFileItemCommand) {
                    // write pre-action user code here
                    switchDisplayable(null, getLocationsFileBrowser());
                    // write post-action user code here
                }
            } else if (displayable == locationsFileBrowser) {
                if (command == FileBrowser.SELECT_FILE_COMMAND) {
                    // write pre-action user code here
                    // write post-action user code here
                } else if (command == cancelCommand2) {
                    // write pre-action user code here
                    switchDisplayable(null, getLocationSettings());
                    // write post-action user code here
                } else if (command == okCommand2) {
                    // write pre-action user code here
                    try {
                        m4();
                    } catch (Throwable e) {
                        C17.m1(e);
                        this.f6 = true;
                    }
                    method();
                    // write post-action user code here
                }
            } else if (displayable == settingsList) {
                if (command == List.SELECT_COMMAND) {
                    // write pre-action user code here

                    settingsListAction();
                    // write post-action user code here
                } else if (command == backCommand3) {
                    // write pre-action user code here
                    switchDisplayable(null, getMainPage());
                    // write post-action user code here
                }
            } else if (displayable == showingResultForm) {
                if (command == backToMainPageCommand7) {
                    // write pre-action user code here
                    switchDisplayable(null, getMainPage());
                    // write post-action user code here
                } else if (command == resetBackCommand6) {
                    // write pre-action user code here
                    switchDisplayable(null, getInputData());
                    // write post-action user code here
                }
            }
            // write post-action user code here
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: exitCommand ">
        /**
         * Returns an initiliazed instance of exitCommand component.
         * @return the initialized component instance
         */
        public Command getExitCommand() {
            if (exitCommand == null) {
                // write pre-init user code here
                exitCommand = new Command("Exit", Command.EXIT, 10);
                // write post-init user code here
            }
            return exitCommand;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: MainPage ">
        /**
         * Returns an initiliazed instance of MainPage component.
         * @return the initialized component instance
         */
        public Form getMainPage() {
            if (MainPage == null) {
                // write pre-init user code here
                MainPage = new Form("Oughat shar\'e shia", new Item[]{getStringItem()});
                MainPage.addCommand(getExitCommand());
                MainPage.addCommand(getHelpCommand());
                MainPage.addCommand(getNextCommand());
                MainPage.addCommand(getGoSettingsCommand());
                MainPage.setCommandListener(this);
                // write post-init user code here
            }
            return MainPage;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: stringItem ">
        /**
         * Returns an initiliazed instance of stringItem component.
         * @return the initialized component instance
         */
        public StringItem getStringItem() {
            if (stringItem == null) {
                // write pre-init user code here
                stringItem = new StringItem("Welcom", "Welcom to \"Oughat\" application! ", Item.PLAIN);
                // write post-init user code here
            }
            return stringItem;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: helpCommand ">
        /**
         * Returns an initiliazed instance of helpCommand component.
         * @return the initialized component instance
         */
        public Command getHelpCommand() {
            if (helpCommand == null) {
                // write pre-init user code here
                helpCommand = new Command("Help", Command.HELP, 10);
                // write post-init user code here
            }
            return helpCommand;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: ticker ">
        /**
         * Returns an initiliazed instance of ticker component.
         * @return the initialized component instance
         */
        public Ticker getTicker() {
            if (ticker == null) {
                // write pre-init user code here
                ticker = new Ticker("\u0628\u0631\u0627\u06CC \u0627\u0637\u0644\u0627\u0639 \u0627\u0632 \u0646\u062D\u0648\u0647 \u06CC \u0645\u062D\u0627\u0633\u0628\u0647 \u0627\u0648\u0642\u0627\u062A \u0628\u0647 \u0642\u0633\u0645\u062A \"\u062F\u0631\u0628\u0627\u0631\u0647 \u06CC \u0628\u0631\u0646\u0627\u0645\u0647 \u0645\u0631\u0627\u062C\u0639\u0647 \u06A9\u0646\u06CC\u062F\"");
                // write post-init user code here
            }
            return ticker;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: nextCommand ">
        /**
         * Returns an initiliazed instance of nextCommand component.
         * @return the initialized component instance
         */
        public Command getNextCommand() {
            if (nextCommand == null) {
                // write pre-init user code here
                nextCommand = new Command("Calculations", Command.OK, 0);
                // write post-init user code here
            }
            return nextCommand;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: goSettingsCommand ">
        /**
         * Returns an initiliazed instance of goSettingsCommand component.
         * @return the initialized component instance
         */
        public Command getGoSettingsCommand() {
            if (goSettingsCommand == null) {
                // write pre-init user code here
                goSettingsCommand = new Command("Settings", Command.OK, 10);
                // write post-init user code here
            }
            return goSettingsCommand;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: cancelCommand ">
        /**
         * Returns an initiliazed instance of cancelCommand component.
         * @return the initialized component instance
         */
        public Command getCancelCommand() {
            if (cancelCommand == null) {
                // write pre-init user code here
                cancelCommand = new Command("Back to main page", Command.CANCEL, 0);
                // write post-init user code here
            }
            return cancelCommand;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: oKCommand ">
        /**
         * Returns an initiliazed instance of oKCommand component.
         * @return the initialized component instance
         */
        public Command getOKCommand() {
            if (oKCommand == null) {
                // write pre-init user code here
                oKCommand = new Command("Ok", Command.OK, 0);
                // write post-init user code here
            }
            return oKCommand;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: altitudeSettings ">
        /**
         * Returns an initiliazed instance of altitudeSettings component.
         * @return the initialized component instance
         */
        public Form getAltitudeSettings() {
            if (altitudeSettings == null) {
                // write pre-init user code here
                altitudeSettings = new Form("Altitude Settings", new Item[]{getTextField(), getTextField2(), getTextField3(), getTextField11(), getTextField1(), getTextField4(), getStringItem9()});
                altitudeSettings.addCommand(getCancelCommand());
                altitudeSettings.addCommand(getOKCommand());
                altitudeSettings.addCommand(getLoadFileCommand());
                altitudeSettings.setCommandListener(this);
                // write post-init user code here
            }
            return altitudeSettings;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: textField ">
        /**
         * Returns an initiliazed instance of textField component.
         * @return the initialized component instance
         */
        public TextField getTextField() {
            if (textField == null) {
                // write pre-init user code here
                textField = new TextField("Azan sobh altitude", Double.toString(C99.u(this.f3.m4())), 32, TextField.DECIMAL);
                // write post-init user code here
            }
            return textField;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: textField1 ">
        /**
         * Returns an initiliazed instance of textField1 component.
         * @return the initialized component instance
         */
        public TextField getTextField1() {
            if (textField1 == null) {
                // write pre-init user code here
                textField1 = new TextField("Azane maghreb altitude", Double.toString(C99.u(f3.m3())), 32, TextField.DECIMAL);
                // write post-init user code here
            }
            return textField1;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: textField2 ">
        /**
         * Returns an initiliazed instance of textField2 component.
         * @return the initialized component instance
         */
        public TextField getTextField2() {
            if (textField2 == null) {
                // write pre-init user code here
                textField2 = new TextField("Sunrise altitude", Double.toString(C99.u(f3.m6())), 32, TextField.DECIMAL);
                // write post-init user code here
            }
            return textField2;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: textField3 ">
        /**
         * Returns an initiliazed instance of textField3 component.
         * @return the initialized component instance
         */
        public TextField getTextField3() {
            if (textField3 == null) {
                // write pre-init user code here
                textField3 = new TextField("Sunset altitude", Double.toString(C99.u(f3.m7())), 32, TextField.DECIMAL);
                // write post-init user code here
            }
            return textField3;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: textField4 ">
        /**
         * Returns an initiliazed instance of textField4 component.
         * @return the initialized component instance
         */
        public TextField getTextField4() {
            if (textField4 == null) {
                // write pre-init user code here
                textField4 = new TextField("Azane esha altitude", Double.toString(C99.u(f3.m5())), 32, TextField.DECIMAL);
                // write post-init user code here
            }
            return textField4;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: okCommand1 ">
        /**
         * Returns an initiliazed instance of okCommand1 component.
         * @return the initialized component instance
         */
        public Command getOkCommand1() {
            if (okCommand1 == null) {
                // write pre-init user code here
                okCommand1 = new Command("Ok", Command.OK, 0);
                // write post-init user code here
            }
            return okCommand1;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: backCommand1 ">
        /**
         * Returns an initiliazed instance of backCommand1 component.
         * @return the initialized component instance
         */
        public Command getBackCommand1() {
            if (backCommand1 == null) {
                // write pre-init user code here
                backCommand1 = new Command("Back", Command.BACK, 10);
                // write post-init user code here
            }
            return backCommand1;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: inputData ">
        /**
         * Returns an initiliazed instance of inputData component.
         * @return the initialized component instance
         */
        public Form getInputData() {
            if (inputData == null) {
                // write pre-init user code here
                inputData = new Form("Fill the blanks:", new Item[]{getTextField5(), getSpacer(), getChoiceGroup1(), getChoiceGroup(), getTextField6(), getSpacer1(), getTextField7(), getTextField8(), getTextField9()});
                inputData.addCommand(getOkCommand1());
                inputData.addCommand(getBackCommand1());
                inputData.addCommand(getShowLocationsItemCommand());
                inputData.setCommandListener(this);
                // write post-init user code here
            }
            return inputData;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: textField5 ">
        /**
         * Returns an initiliazed instance of textField5 component.
         * @return the initialized component instance
         */
        public TextField getTextField5() {
            if (textField5 == null) {
                // write pre-init user code here
                textField5 = new TextField("Input day, mounth, year(ex: 1 1 1389) :", null, 32, TextField.ANY);
                // write post-init user code here
            }
            return textField5;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: textField6 ">
        /**
         * Returns an initiliazed instance of textField6 component.
         * @return the initialized component instance
         */
        public TextField getTextField6() {
            if (textField6 == null) {
                // write pre-init user code here
                textField6 = new TextField("Input name of location:", null, 32, TextField.ANY);
                // write post-init user code here
            }
            return textField6;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: altitudeSettingsCommand ">
        /**
         * Returns an initiliazed instance of altitudeSettingsCommand component.
         * @return the initialized component instance
         */
        public Command getAltitudeSettingsCommand() {
            if (altitudeSettingsCommand == null) {
                // write pre-init user code here
                altitudeSettingsCommand = new Command("Altitude settings", Command.OK, 0);
                // write post-init user code here
            }
            return altitudeSettingsCommand;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: backCommand2 ">
        /**
         * Returns an initiliazed instance of backCommand2 component.
         * @return the initialized component instance
         */
        public Command getBackCommand2() {
            if (backCommand2 == null) {
                // write pre-init user code here
                backCommand2 = new Command("Back", Command.BACK, 0);
                // write post-init user code here
            }
            return backCommand2;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: backCommand3 ">
        /**
         * Returns an initiliazed instance of backCommand3 component.
         * @return the initialized component instance
         */
        public Command getBackCommand3() {
            if (backCommand3 == null) {
                // write pre-init user code here
                backCommand3 = new Command("Back", Command.BACK, 0);
                // write post-init user code here
            }
            return backCommand3;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: loadFileCommand ">
        /**
         * Returns an initiliazed instance of loadFileCommand component.
         * @return the initialized component instance
         */
        public Command getLoadFileCommand() {
            if (loadFileCommand == null) {
                // write pre-init user code here
                loadFileCommand = new Command("Load from file", Command.ITEM, 0);
                // write post-init user code here
            }
            return loadFileCommand;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: okCommand ">
        /**
         * Returns an initiliazed instance of okCommand component.
         * @return the initialized component instance
         */
        public Command getOkCommand() {
            if (okCommand == null) {
                // write pre-init user code here
                okCommand = new Command("Ok", Command.OK, 1);
                // write post-init user code here
            }
            return okCommand;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: cancelCommand1 ">
        /**
         * Returns an initiliazed instance of cancelCommand1 component.
         * @return the initialized component instance
         */
        public Command getCancelCommand1() {
            if (cancelCommand1 == null) {
                // write pre-init user code here
                cancelCommand1 = new Command("Cancel", Command.CANCEL, 1);
                // write post-init user code here
            }
            return cancelCommand1;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: settingsList ">
        /**
         * Returns an initiliazed instance of settingsList component.
         * @return the initialized component instance
         */
        public List getSettingsList() {
            if (settingsList == null) {
                // write pre-init user code here
                settingsList = new List("list", Choice.IMPLICIT);
                settingsList.append("Altitude settings", null);
                settingsList.append("organize Locations", null);
                settingsList.addCommand(getBackCommand3());
                settingsList.setCommandListener(this);
                settingsList.setSelectedFlags(new boolean[]{false, false});
                // write post-init user code here
            }
            return settingsList;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Method: settingsListAction ">
        /**
         * Performs an action assigned to the selected list element in the settingsList component.
         */
        public void settingsListAction() {
            // enter pre-action user code here
            String __selectedString = getSettingsList().getString(getSettingsList().getSelectedIndex());
            if (__selectedString != null) {
                if (__selectedString.equals("Altitude settings")) {
                    // write pre-action user code here
                    if (this.f3.m15()) {
                        getTextField11().setString("s" + Double.toString(this.f3.m16()));
                    } else {
                        getTextField11().setString(Double.toString(this.f3.m16()));
                    }

                    switchDisplayable(null, getAltitudeSettings());
                    // write post-action user code here
                } else if (__selectedString.equals("organize Locations")) {
                    // write pre-action user code here
                    switchDisplayable(null, getLocationSettings());
                    // write post-action user code here
                }
            }
            // enter post-action user code here
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: altitudeFileBrowser ">
        /**
         * Returns an initiliazed instance of altitudeFileBrowser component.
         * @return the initialized component instance
         */
        public FileBrowser getAltitudeFileBrowser() {
            if (altitudeFileBrowser == null) {
                // write pre-init user code here
                altitudeFileBrowser = new FileBrowser(getDisplay());
                altitudeFileBrowser.setTitle("Select file");
                altitudeFileBrowser.setCommandListener(this);
                altitudeFileBrowser.setFilter("y");
                altitudeFileBrowser.addCommand(FileBrowser.SELECT_FILE_COMMAND);
                altitudeFileBrowser.addCommand(getOkCommand());
                altitudeFileBrowser.addCommand(getCancelCommand1());
                // write post-init user code here
            }
            return altitudeFileBrowser;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Method: altitudeLoaderMethod ">
        /**
         * Performs an action assigned to the altitudeLoaderMethod if-point.
         */
        public void m1() {
            // enter pre-if user code here
            if (this.f1) {
                // write pre-action user code here
                this.f1 = false;
                switchDisplayable(null, getAlert());
                // write post-action user code here
            } else {
                // write pre-action user code here
                switchDisplayable(null, getAltitudeSettings());
                // write post-action user code here
            }
            // enter post-if user code here
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: alert ">
        /**
         * Returns an initiliazed instance of alert component.
         * @return the initialized component instance
         */
        public Alert getAlert() {
            if (alert == null) {
                // write pre-init user code here
                alert = new Alert("alert", "The selected file is not useful for application", null, AlertType.ERROR);
                alert.addCommand(getTryEgainBackCommand());
                alert.addCommand(getBackToAltitudeSettCommand());
                alert.setCommandListener(this);
                alert.setTimeout(Alert.FOREVER);
                // write post-init user code here
            }
            return alert;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: tryEgainBackCommand ">
        /**
         * Returns an initiliazed instance of tryEgainBackCommand component.
         * @return the initialized component instance
         */
        public Command getTryEgainBackCommand() {
            if (tryEgainBackCommand == null) {
                // write pre-init user code here
                tryEgainBackCommand = new Command("Try again", Command.BACK, 0);
                // write post-init user code here
            }
            return tryEgainBackCommand;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: lCommand2 ">
        /**
         * Returns an initiliazed instance of lCommand2 component.
         * @return the initialized component instance
         */
        public Command getLCommand2() {
            if (lCommand2 == null) {
                // write pre-init user code here
                lCommand2 = new Command("Cancel", Command.CANCEL, 0);
                // write post-init user code here
            }
            return lCommand2;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: backToAltitudeSettCommand ">
        /**
         * Returns an initiliazed instance of backToAltitudeSettCommand component.
         * @return the initialized component instance
         */
        public Command getBackToAltitudeSettCommand() {
            if (backToAltitudeSettCommand == null) {
                // write pre-init user code here
                backToAltitudeSettCommand = new Command("Back to altitude settings", Command.BACK, 0);
                // write post-init user code here
            }
            return backToAltitudeSettCommand;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: backCommand ">
        /**
         * Returns an initiliazed instance of backCommand component.
         * @return the initialized component instance
         */
        public Command getBackCommand() {
            if (backCommand == null) {
                // write pre-init user code here
                backCommand = new Command("Back", Command.BACK, 0);
                // write post-init user code here
            }
            return backCommand;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: loadFromFileItemCommand ">
        /**
         * Returns an initiliazed instance of loadFromFileItemCommand component.
         * @return the initialized component instance
         */
        public Command getLoadFromFileItemCommand() {
            if (loadFromFileItemCommand == null) {
                // write pre-init user code here
                loadFromFileItemCommand = new Command("Load from file", Command.ITEM, 0);
                // write post-init user code here
            }
            return loadFromFileItemCommand;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: locationSettings ">
        /**
         * Returns an initiliazed instance of locationSettings component.
         * @return the initialized component instance
         */
        public Form getLocationSettings() {
            if (locationSettings == null) {
                // write pre-init user code here
                locationSettings = new Form("Organize Location");
                locationSettings.addCommand(getBackCommand());
                locationSettings.addCommand(getLoadFromFileItemCommand());
                locationSettings.setCommandListener(this);
                // write post-init user code here
            }
            return locationSettings;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: locationsFileBrowser ">
        /**
         * Returns an initiliazed instance of locationsFileBrowser component.
         * @return the initialized component instance
         */
        public FileBrowser getLocationsFileBrowser() {
            if (locationsFileBrowser == null) {
                // write pre-init user code here
                locationsFileBrowser = new FileBrowser(getDisplay());
                locationsFileBrowser.setTitle("fileBrowser");
                locationsFileBrowser.setCommandListener(this);
                locationsFileBrowser.addCommand(FileBrowser.SELECT_FILE_COMMAND);
                locationsFileBrowser.addCommand(getOkCommand2());
                locationsFileBrowser.addCommand(getCancelCommand2());
                // write post-init user code here
            }
            return locationsFileBrowser;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: okCommand2 ">
        /**
         * Returns an initiliazed instance of okCommand2 component.
         * @return the initialized component instance
         */
        public Command getOkCommand2() {
            if (okCommand2 == null) {
                // write pre-init user code here
                okCommand2 = new Command("Ok", Command.OK, 0);
                // write post-init user code here
            }
            return okCommand2;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: cancelCommand2 ">
        /**
         * Returns an initiliazed instance of cancelCommand2 component.
         * @return the initialized component instance
         */
        public Command getCancelCommand2() {
            if (cancelCommand2 == null) {
                // write pre-init user code here
                cancelCommand2 = new Command("Cancel", Command.CANCEL, 0);
                // write post-init user code here
            }
            return cancelCommand2;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Method: method ">
        /**
         * Performs an action assigned to the method if-point.
         */
        public void method() {
            // enter pre-if user code here
            if (this.f6) {
                // write pre-action user code here
                this.f6 = false;
                switchDisplayable(null, getAlert1());
                // write post-action user code here
            } else {
                // write pre-action user code here
                switchDisplayable(null, getLocationSettings());
                // write post-action user code here
            }
            // enter post-if user code here
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: tryAgainCancelCommand3 ">
        /**
         * Returns an initiliazed instance of tryAgainCancelCommand3 component.
         * @return the initialized component instance
         */
        public Command getTryAgainCancelCommand3() {
            if (tryAgainCancelCommand3 == null) {
                // write pre-init user code here
                tryAgainCancelCommand3 = new Command("Try again", Command.CANCEL, 0);
                // write post-init user code here
            }
            return tryAgainCancelCommand3;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: cancelCommand3 ">
        /**
         * Returns an initiliazed instance of cancelCommand3 component.
         * @return the initialized component instance
         */
        public Command getCancelCommand3() {
            if (cancelCommand3 == null) {
                // write pre-init user code here
                cancelCommand3 = new Command("Cancel", Command.CANCEL, 0);
                // write post-init user code here
            }
            return cancelCommand3;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: backCommand4 ">
        /**
         * Returns an initiliazed instance of backCommand4 component.
         * @return the initialized component instance
         */
        public Command getBackCommand4() {
            if (backCommand4 == null) {
                // write pre-init user code here
                backCommand4 = new Command("Back to location settings", Command.BACK, 0);
                // write post-init user code here
            }
            return backCommand4;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: alert1 ">
        /**
         * Returns an initiliazed instance of alert1 component.
         * @return the initialized component instance
         */
        public Alert getAlert1() {
            if (alert1 == null) {
                // write pre-init user code here
                alert1 = new Alert("alert1");
                alert1.addCommand(getTryAgainCancelCommand3());
                alert1.addCommand(getBackCommand4());
                alert1.setCommandListener(this);
                alert1.setTimeout(Alert.FOREVER);
                // write post-init user code here
            }
            return alert1;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: textField7 ">
        /**
         * Returns an initiliazed instance of textField7 component.
         * @return the initialized component instance
         */
        public TextField getTextField7() {
            if (textField7 == null) {
                // write pre-init user code here
                textField7 = new TextField("Input latiotude of location:", null, 32, TextField.ANY);
                // write post-init user code here
            }
            return textField7;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: textField8 ">
        /**
         * Returns an initiliazed instance of textField8 component.
         * @return the initialized component instance
         */
        public TextField getTextField8() {
            if (textField8 == null) {
                // write pre-init user code here
                textField8 = new TextField("Input longitude of location:", null, 32, TextField.ANY);
                // write post-init user code here
            }
            return textField8;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: spacer ">
        /**
         * Returns an initiliazed instance of spacer component.
         * @return the initialized component instance
         */
        public Spacer getSpacer() {
            if (spacer == null) {
                // write pre-init user code here
                spacer = new Spacer(16, 3);
                // write post-init user code here
            }
            return spacer;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: textField9 ">
        /**
         * Returns an initiliazed instance of textField9 component.
         * @return the initialized component instance
         */
        public TextField getTextField9() {
            if (textField9 == null) {
                // write pre-init user code here
                textField9 = new TextField("Input time zone of location:", null, 32, TextField.ANY);
                // write post-init user code here
            }
            return textField9;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: spacer1 ">
        /**
         * Returns an initiliazed instance of spacer1 component.
         * @return the initialized component instance
         */
        public Spacer getSpacer1() {
            if (spacer1 == null) {
                // write pre-init user code here
                spacer1 = new Spacer(16, 3);
                spacer1.setLayout(ImageItem.LAYOUT_DEFAULT);
                // write post-init user code here
            }
            return spacer1;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: showingResultForm ">
        /**
         * Returns an initiliazed instance of showingResultForm component.
         * @return the initialized component instance
         */
        public Form getShowingResultForm() {
            if (showingResultForm == null) {
                // write pre-init user code here
                showingResultForm = new Form("Result", new Item[]{getStringItem2(), getStringItem8(), getStringItem3(), getStringItem4(), getStringItem5(), getStringItem6(), getStringItem7(), getStringItem1()});
                showingResultForm.addCommand(getResetBackCommand6());
                showingResultForm.addCommand(getBackToMainPageCommand7());
                showingResultForm.setCommandListener(this);
                // write post-init user code here
            }
            return showingResultForm;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Method: method1 ">
        /**
         * Performs an action assigned to the method1 if-point.
         */
        public void method1() {
            // enter pre-if user code here
            if (this.f7) {
                // write pre-action user code here
                switchDisplayable(null, getAlert2());
                // write post-action user code here
            } else {
                // write pre-action user code herer

                switchDisplayable(null, getShowingResultForm());
                // write post-action user code here
            }
            // enter post-if user code here
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: alert2 ">
        /**
         * Returns an initiliazed instance of alert2 component.
         * @return the initialized component instance
         */
        public Alert getAlert2() {
            if (alert2 == null) {
                // write pre-init user code here
                alert2 = new Alert("Error in data", "The location that you enter its name not found or other location properties are not correct.", null, AlertType.ERROR);
                alert2.addCommand(getBackCommand5());
                alert2.setCommandListener(this);
                alert2.setTimeout(Alert.FOREVER);
                // write post-init user code here
            }
            return alert2;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: backCommand5 ">
        /**
         * Returns an initiliazed instance of backCommand5 component.
         * @return the initialized component instance
         */
        public Command getBackCommand5() {
            if (backCommand5 == null) {
                // write pre-init user code here
                backCommand5 = new Command("Back", Command.BACK, 0);
                // write post-init user code here
            }
            return backCommand5;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: resetBackCommand6 ">
        /**
         * Returns an initiliazed instance of resetBackCommand6 component.
         * @return the initialized component instance
         */
        public Command getResetBackCommand6() {
            if (resetBackCommand6 == null) {
                // write pre-init user code here
                resetBackCommand6 = new Command("Reset data", Command.BACK, 0);
                // write post-init user code here
            }
            return resetBackCommand6;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: backToMainPageCommand7 ">
        /**
         * Returns an initiliazed instance of backToMainPageCommand7 component.
         * @return the initialized component instance
         */
        public Command getBackToMainPageCommand7() {
            if (backToMainPageCommand7 == null) {
                // write pre-init user code here
                backToMainPageCommand7 = new Command("Main page", Command.BACK, 0);
                // write post-init user code here
            }
            return backToMainPageCommand7;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: stringItem2 ">
        /**
         * Returns an initiliazed instance of stringItem2 component.
         * @return the initialized component instance
         */
        public StringItem getStringItem2() {
            if (stringItem2 == null) {
                // write pre-init user code here
                stringItem2 = new StringItem("Fajr:", null);
                // write post-init user code here
            }
            return stringItem2;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: stringItem3 ">
        /**
         * Returns an initiliazed instance of stringItem3 component.
         * @return the initialized component instance
         */
        public StringItem getStringItem3() {
            if (stringItem3 == null) {
                // write pre-init user code here
                stringItem3 = new StringItem("Zohr:", null);
                // write post-init user code here
            }
            return stringItem3;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: stringItem4 ">
        /**
         * Returns an initiliazed instance of stringItem4 component.
         * @return the initialized component instance
         */
        public StringItem getStringItem4() {
            if (stringItem4 == null) {
                // write pre-init user code here
                stringItem4 = new StringItem("Asr:", null);
                // write post-init user code here
            }
            return stringItem4;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: stringItem5 ">
        /**
         * Returns an initiliazed instance of stringItem5 component.
         * @return the initialized component instance
         */
        public StringItem getStringItem5() {
            if (stringItem5 == null) {
                // write pre-init user code here
                stringItem5 = new StringItem("Sunset", null);
                // write post-init user code here
            }
            return stringItem5;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: stringItem6 ">
        /**
         * Returns an initiliazed instance of stringItem6 component.
         * @return the initialized component instance
         */
        public StringItem getStringItem6() {
            if (stringItem6 == null) {
                // write pre-init user code here
                stringItem6 = new StringItem("Maghreb:", null);
                // write post-init user code here
            }
            return stringItem6;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: stringItem7 ">
        /**
         * Returns an initiliazed instance of stringItem7 component.
         * @return the initialized component instance
         */
        public StringItem getStringItem7() {
            if (stringItem7 == null) {
                // write pre-init user code here
                stringItem7 = new StringItem("Esha:", null);
                // write post-init user code here
            }
            return stringItem7;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: stringItem8 ">
        /**
         * Returns an initiliazed instance of stringItem8 component.
         * @return the initialized component instance
         */
        public StringItem getStringItem8() {
            if (stringItem8 == null) {
                // write pre-init user code here
                stringItem8 = new StringItem("Sunrise:", null);
                // write post-init user code here
            }
            return stringItem8;
        }
        //</editor-fold>

        // enter pre-action user code here
        // enter post-action user code here
        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: showLocationsItemCommand ">
        /**
         * Returns an initiliazed instance of showLocationsItemCommand component.
         * @return the initialized component instance
         */
        public Command getShowLocationsItemCommand() {
            if (showLocationsItemCommand == null) {
                // write pre-init user code here
                showLocationsItemCommand = new Command("Search location", Command.ITEM, 0);
                // write post-init user code here
            }
            return showLocationsItemCommand;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: cancelCommand4 ">
        /**
         * Returns an initiliazed instance of cancelCommand4 component.
         * @return the initialized component instance
         */
        public Command getCancelCommand4() {
            if (cancelCommand4 == null) {
                // write pre-init user code here
                cancelCommand4 = new Command("Cancel", Command.CANCEL, 0);
                // write post-init user code here
            }
            return cancelCommand4;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: searchOkCommand3 ">
        /**
         * Returns an initiliazed instance of searchOkCommand3 component.
         * @return the initialized component instance
         */
        public Command getSearchOkCommand3() {
            if (searchOkCommand3 == null) {
                // write pre-init user code here
                searchOkCommand3 = new Command("Search", Command.OK, 0);
                // write post-init user code here
            }
            return searchOkCommand3;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: inputFirstOfLocation ">
        /**
         * Returns an initiliazed instance of inputFirstOfLocation component.
         * @return the initialized component instance
         */
        public Form getInputFirstOfLocation() {
            if (inputFirstOfLocation == null) {
                // write pre-init user code here
                inputFirstOfLocation = new Form("Input location", new Item[]{getTextField10()});
                inputFirstOfLocation.addCommand(getCancelCommand4());
                inputFirstOfLocation.addCommand(getSearchOkCommand3());
                inputFirstOfLocation.setCommandListener(this);
                // write post-init user code here
            }
            return inputFirstOfLocation;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: textField10 ">
        /**
         * Returns an initiliazed instance of textField10 component.
         * @return the initialized component instance
         */
        public TextField getTextField10() {
            if (textField10 == null) {
                // write pre-init user code here
                textField10 = new TextField("Input first letters of location:", null, 32, TextField.ANY);
                // write post-init user code here
            }
            return textField10;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: choiceGroup1 ">
        /**
         * Returns an initiliazed instance of choiceGroup1 component.
         * @return the initialized component instance
         */
        public ChoiceGroup getChoiceGroup1() {
            if (choiceGroup1 == null) {
                // write pre-init user code here
                choiceGroup1 = new ChoiceGroup("Type of input data:", Choice.POPUP);
                choiceGroup1.append("Location name", null);
                choiceGroup1.append("Location properties", null);
                choiceGroup1.setSelectedFlags(new boolean[]{false, false});
                // write post-init user code here
            }
            return choiceGroup1;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: helpPage0 ">
        /**
         * Returns an initiliazed instance of helpPage0 component.
         * @return the initialized component instance
         */
        public TextBox getHelpPage0() {
            if (helpPage0 == null) {
                // write pre-init user code here
                helpPage0 = new TextBox("\u062A\u0648\u0636\u06CC\u062D\u0627\u062A", "\u0645\u062D\u0627\u0633\u0628\u0627\u062A \u0627\u06CC\u0646 \u0645\u062C\u0645\u0648\u0639\u0647 \u0637\u0648\u0631\u06CC \n\u0637\u0631\u0627\u062D\u06CC \u0634\u062F\u0647 \u0627\u0633\u062A \u06A9\u0647 \u0645\u0637\u0627\u0628\u0642 \n\u0628\u0627 \u0646\u062A\u0627\u06CC\u062C\u06CC \u0628\u0627\u0634\u062F \u06A9\u0647 \u062F\u0631\n \u0634\u0628\u06A9\u0647 \u0647\u0627\u06CC \u0631\u0633\u0645\u06CC \u062A\u0644\u0648\u06CC\u0632\u06CC\u0648\u0646\n \u062C\u0645\u0647\u0648\u0631\u06CC \u0627\u0633\u0644\u0627\u0645\u06CC \u0627\u06CC\u0631\u0627\u0646 \u0627\u0639\u0644\u0627\u0645 \u0645\u06CC \u0634\u0648\u062F.\n\u062F\u0631 \u0645\u0648\u0631\u062F \u0627\u0630\u0627\u0646 \u0639\u0635\u0631 \u0648 \u0639\u0634\u0627 \u060C \u0645\u062D\u0627\u0633\u0628\u0647 \u0645\u0637\u0627\u0628\u0642 \n\u0628\u0627 \u0641\u062A\u0648\u0627\u06CC \u0627\u0645\u0627\u0645 \u062E\u0645\u06CC\u0646\u06CC \n\u062F\u0631 \u0645\u0648\u0631\u062F \u0648\u0642\u062A \u0641\u0636\u06CC\u0644\u062A \u0627\u06CC\u0646 \u0646\u0645\u0627\u0632 \u0627\u0633\u062A.\n\u0646\u06CC\u0645\u0647 \u0634\u0628 \u0634\u0631\u0639\u06CC \u0647\u0645 \u0637\u0628\u0642 \u0631\u0633\u0627\u0644\u0647 \u06CC \n\u0627\u0645\u0627\u0645 \u062E\u0645\u06CC\u0646\u06CC \u060C \u0627\u0630\u0627\u0646 \u0638\u0647\u0631 \u0628\u0647 \u0639\u0644\u0627\u0648\u0647 \u06CC \n\u06CC\u0627\u0632\u062F\u0647 \u0633\u0627\u0639\u062A \u0648 \u0631\u0628\u0639 \u0628\u0647 \u062F\u0633\u062A \u0645\u06CC \u0622\u06CC\u062F \u0627\u0644\u0628\u062A\u0647 \n\u0627\u06CC\u0646 \u0642\u0633\u0645\u062A \u0645\u0627\u0646\u0646\u062F \u0642\u0633\u0645\u062A \u0647\u0627\u06CC \u062F\u06CC\u06AF\u0631\n \u062F\u0642\u062A \u0642\u0627\u0628\u0644 \u0642\u0628\u0648\u0644\u06CC \u0646\u062F\u0627\u0631\u062F.  \n\u0628\u0647 \u062C\u0632 \u0627\u06CC\u0646 \u0645\u0648\u0631\u062F \u060C \u062F\u0642\u062A \u062F\u0631 \u0627\u0630\u0627\u0646 \u0635\u0628\u062D 3 \u062F\u0642\u06CC\u0642\u0647 \u060C \n\u062F\u0631 \u0637\u0644\u0648\u0639 \u0622\u0641\u062A\u0627\u0628 2 \u062F\u0642\u06CC\u0642\u0647 \u060C \u062F\u0631 \u0627\u0630\u0627\u0646 \u0638\u0647\u0631 30 \u062B\u0627\u0646\u06CC\u0647 \n\u060C\u062F\u0631 \u0648\u0642\u062A \u0641\u0636\u06CC\u0644\u062A \u0646\u0645\u0627\u0632 \u0639\u0635\u0631 1 \u062F\u0642\u06CC\u0642\u0647 \u060C \u062F\u0631 \u063A\u0631\u0648\u0628 \n\u0622\u0641\u062A\u0627\u0628 2 \u062F\u0642\u06CC\u0642\u0647 \u060C\u062F\u0631 \u0627\u0630\u0627\u0646 \u0645\u063A\u0631\u0628 3 \u062F\u0642\u06CC\u0642\u0647 \n\u0648 \u062F\u0631 \u0648\u0642\u062A \u0641\u0636\u06CC\u0644\u062A \u0646\u0645\u0627\u0632 \u0639\u0634\u0627 3 \u062F\u0642\u06CC\u0642\u0647 \u0645\u06CC \u0628\u0627\u0634\u062F.\n\u062F\u0631 \u0635\u0648\u0631\u062A\u06CC \u06A9\u0647 \u0634\u0627\u0647\u062F \u0646\u062A\u06CC\u062C\u0647 \u0627\u06CC \u0628\u0627 \u062F\u0642\u062A \u0628\u062F\u062A\u0631 \n\u0627\u0632 \u0622\u0646\u0686\u0647 \u06CC\u0627\u062F \u0634\u062F\u060C \u0628\u0648\u062F\u06CC\u062F \u0644\u0637\u0641\u0627 \u0622\u0646 \u0645\u0648\u0631\u062F \u0631\u0627 \n\u0628\u0627 \u062C\u0632\u06CC\u06CC\u0627\u062A\u0634 \u0628\u0627 \u067E\u0633\u062A \u0627\u0644\u06A9\u062A\u0631\u0648\u0646\u06CC\u06A9\u06CC \nought1389@gmail.com\n\u062F\u0631 \u0645\u06CC\u0627\u0646 \u0628\u06AF\u0630\u0627\u0631\u06CC\u062F.\n\u062A\u0648\u062C\u0647 \u062F\u0627\u0634\u062A\u0647 \u0628\u0627\u0634\u06CC\u062F\n \u0646\u062A\u0627\u06CC\u062C \u0645\u062D\u0627\u0633\u0628\u0647 \u0645\u0637\u0627\u0628\u0642 \u0628\u0627 \u0633\u0627\u0639\u062A\n \u0631\u0633\u0645\u06CC \u06A9\u0634\u0648\u0631\u06CC \u0627\u0633\u062A \u06A9\u0647 \n\u0622\u0646 \u0645\u06A9\u0627\u0646 \u062F\u0631 \u0622\u0646 \u0642\u0631\u0627\u0631 \u062F\u0627\u0631\u062F \u0627\u0633\u062A. \n\u0627\u06AF\u0631 \u0645\u06CC\u062E\u0648\u0627\u0647\u06CC\u062F \u0632\u0645\u0627\u0646 \u0628\u0631 \u062D\u0633\u0628 GMT \u0628\u0627\u0634\u062F\n\u062F\u0631 \u062C\u0627\u06CC\u06CC \u06A9\u0647 \u062F\u0627\u062F\u0647 \u0647\u0627 \u0631\u0627 \u0648\u0627\u0631\u062F \u0645\u06CC \u06A9\u0646\u06CC\u062F \u062C\u0644\u0648\u06CC  \nIgnore Time Zone \n\u062A\u06CC\u06A9 \u0628\u0632\u0646\u06CC\u062F. ", 500, TextField.ANY | TextField.UNEDITABLE);
                // write post-init user code here
            }
            return helpPage0;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: stringItem1 ">
        /**
         * Returns an initiliazed instance of stringItem1 component.
         * @return the initialized component instance
         */
        public StringItem getStringItem1() {
            if (stringItem1 == null) {
                // write pre-init user code here
                stringItem1 = new StringItem("Nime shabe sharei:", null);
                // write post-init user code here
            }
            return stringItem1;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: choiceGroup ">
        /**
         * Returns an initiliazed instance of choiceGroup component.
         * @return the initialized component instance
         */
        public ChoiceGroup getChoiceGroup() {
            if (choiceGroup == null) {
                // write pre-init user code here
                choiceGroup = new ChoiceGroup("Type of calculations:", Choice.MULTIPLE);
                choiceGroup.append("Ignore Time Zone", null);
                choiceGroup.append("Ignore Summer Added Houre", null);
                choiceGroup.setSelectedFlags(new boolean[]{false, false});
                // write post-init user code here
            }
            return choiceGroup;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: textField11 ">
        /**
         * Returns an initiliazed instance of textField11 component.
         * @return the initialized component instance
         */
        public TextField getTextField11() {
            if (textField11 == null) {
                // write pre-init user code here
                textField11 = new TextField("Asr sundial shadow ratio:", null, 32, TextField.ANY);
                // write post-init user code here
            }
            return textField11;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: stringItem9 ">
        /**
         * Returns an initiliazed instance of stringItem9 component.
         * @return the initialized component instance
         */
        public StringItem getStringItem9() {
            if (stringItem9 == null) {
                // write pre-init user code here
                stringItem9 = new StringItem("Hint:", "For \"asr sundial ...\" ; input with a \'s\' at first of float number means: <<when shadow of sundial reach at its value at zohr plus the ratio that you entered>> otherwise (without any charactor at first) means :<<when the shadow of sundial reach at ratio that you enter>>\n ");
                // write post-init user code here
            }
            return stringItem9;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: globalBack ">
        /**
         * Returns an initiliazed instance of globalBack component.
         * @return the initialized component instance
         */
        public Command getGlobalBack() {
            if (globalBack == null) {
                // write pre-init user code here
                globalBack = new Command("Back", Command.BACK, 0);
                // write post-init user code here
            }
            return globalBack;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: helpPage ">
        /**
         * Returns an initiliazed instance of helpPage component.
         * @return the initialized component instance
         */
        public Form getHelpPage() {
            if (helpPage == null) {
                // write pre-init user code here
                helpPage = new Form("\u0631\u0627\u0647\u0646\u0645\u0627", new Item[]{getStringItem10()});
                helpPage.addCommand(getGlobalBack());
                helpPage.setCommandListener(this);
                // write post-init user code here
            }
            return helpPage;
        }
        //</editor-fold>

        //<editor-fold defaultstate="collapsed" desc=" Generated Getter: stringItem10 ">
        /**
         * Returns an initiliazed instance of stringItem10 component.
         * @return the initialized component instance
         */
        public StringItem getStringItem10() {
            if (stringItem10 == null) {
                // write pre-init user code here
                stringItem10 = new StringItem("", "\u0645\u062D\u0627\u0633\u0628\u0627\u062A \u0627\u06CC\u0646 \u0645\u062C\u0645\u0648\u0639\u0647 \u0637\u0648\u0631\u06CC \u0637\u0631\u0627\u062D\u06CC \u0634\u062F\u0647 \u0627\u0633\u062A \u06A9\u0647 \u0645\u0637\u0627\u0628\u0642 \u0628\u0627 \u0646\u062A\u0627\u06CC\u062C\u06CC \u0628\u0627\u0634\u062F \u06A9\u0647 \u062F\u0631 \u0634\u0628\u06A9\u0647 \u0647\u0627\u06CC \u0631\u0633\u0645\u06CC \u062A\u0644\u0648\u06CC\u0632\u06CC\u0648\u0646 \u062C\u0645\u0647\u0648\u0631\u06CC \u0627\u0633\u0644\u0627\u0645\u06CC \u0627\u06CC\u0631\u0627\u0646 \u0627\u0639\u0644\u0627\u0645 \u0645\u06CC \u0634\u0648\u062F.\n\u062F\u0631 \u0645\u0648\u0631\u062F \u0627\u0630\u0627\u0646 \u0639\u0635\u0631 \u0648 \u0639\u0634\u0627 \u060C \u0645\u062D\u0627\u0633\u0628\u0647 \u0645\u0637\u0627\u0628\u0642 \u0628\u0627 \u0641\u062A\u0648\u0627\u06CC \u0627\u0645\u0627\u0645 \u062E\u0645\u06CC\u0646\u06CC \u062F\u0631 \u0645\u0648\u0631\u062F \u0648\u0642\u062A \u0641\u0636\u06CC\u0644\u062A \u0627\u06CC\u0646 \u0646\u0645\u0627\u0632 \u0627\u0633\u062A.\n\u0646\u06CC\u0645\u0647 \u0634\u0628 \u0634\u0631\u0639\u06CC \u0647\u0645 \u0637\u0628\u0642 \u0631\u0633\u0627\u0644\u0647 \u06CC \u0627\u0645\u0627\u0645 \u062E\u0645\u06CC\u0646\u06CC \u060C \u0627\u0630\u0627\u0646 \u0638\u0647\u0631 \u0628\u0647 \u0639\u0644\u0627\u0648\u0647 \u06CC \u06CC\u0627\u0632\u062F\u0647 \u0633\u0627\u0639\u062A \u0648 \u0631\u0628\u0639 \u0628\u0647 \u062F\u0633\u062A \u0645\u06CC \u0622\u06CC\u062F \u0627\u0644\u0628\u062A\u0647 \u0627\u06CC\u0646 \u0642\u0633\u0645\u062A \u0645\u0627\u0646\u0646\u062F \u0642\u0633\u0645\u062A \u0647\u0627\u06CC \u062F\u06CC\u06AF\u0631 \u062F\u0642\u062A \u0642\u0627\u0628\u0644 \u0642\u0628\u0648\u0644\u06CC \u0646\u062F\u0627\u0631\u062F.  \n\u0628\u0647 \u062C\u0632 \u0627\u06CC\u0646 \u0645\u0648\u0631\u062F \u060C \u062F\u0642\u062A \u062F\u0631 \u0627\u0630\u0627\u0646 \u0635\u0628\u062D 3 \u062F\u0642\u06CC\u0642\u0647 \u060C \u062F\u0631 \u0637\u0644\u0648\u0639 \u0622\u0641\u062A\u0627\u0628 2 \u062F\u0642\u06CC\u0642\u0647 \u060C \u062F\u0631 \u0627\u0630\u0627\u0646 \u0638\u0647\u0631 30 \u062B\u0627\u0646\u06CC\u0647 \u060C\u062F\u0631 \u0648\u0642\u062A \u0641\u0636\u06CC\u0644\u062A \u0646\u0645\u0627\u0632 \u0639\u0635\u0631 1 \u062F\u0642\u06CC\u0642\u0647 \u060C \u062F\u0631 \u063A\u0631\u0648\u0628 \u0622\u0641\u062A\u0627\u0628 2 \u062F\u0642\u06CC\u0642\u0647 \u060C\u062F\u0631 \u0627\u0630\u0627\u0646 \u0645\u063A\u0631\u0628 3 \u062F\u0642\u06CC\u0642\u0647 \u0648 \u062F\u0631 \u0648\u0642\u062A \u0641\u0636\u06CC\u0644\u062A \u0646\u0645\u0627\u0632 \u0639\u0634\u0627 3 \u062F\u0642\u06CC\u0642\u0647 \u0645\u06CC \u0628\u0627\u0634\u062F.\n\u062F\u0631 \u0635\u0648\u0631\u062A\u06CC \u06A9\u0647 \u0634\u0627\u0647\u062F \u0646\u062A\u06CC\u062C\u0647 \u0627\u06CC \u0628\u0627 \u062F\u0642\u062A \u0628\u062F\u062A\u0631 \u0627\u0632 \u0622\u0646\u0686\u0647 \u06CC\u0627\u062F \u0634\u062F\u060C \u0628\u0648\u062F\u06CC\u062F \u0644\u0637\u0641\u0627 \u0622\u0646 \u0645\u0648\u0631\u062F \u0631\u0627 \u0628\u0627 \u062C\u0632\u06CC\u06CC\u0627\u062A\u0634 \u0628\u0627 \u067E\u0633\u062A \u0627\u0644\u06A9\u062A\u0631\u0648\u0646\u06CC\u06A9\u06CC oughat1389@gmail.com\n\u062F\u0631 \u0645\u06CC\u0627\u0646 \u0628\u06AF\u0630\u0627\u0631\u06CC\u062F. \u062A\u0648\u062C\u0647 \u062F\u0627\u0634\u062A\u0647 \u0628\u0627\u0634\u06CC\u062F \u0646\u062A\u0627\u06CC\u062C \u0645\u062D\u0627\u0633\u0628\u0647 \u0645\u0637\u0627\u0628\u0642 \u0628\u0627 \u0633\u0627\u0639\u062A \u0631\u0633\u0645\u06CC \u06A9\u0634\u0648\u0631\u06CC \u0627\u0633\u062A \u06A9\u0647 \u0622\u0646 \u0645\u06A9\u0627\u0646 \u062F\u0631 \u0622\u0646 \u0642\u0631\u0627\u0631 \u062F\u0627\u0631\u062F \u0627\u0633\u062A.\n\u0627\u06AF\u0631 \u0645\u06CC\u062E\u0648\u0627\u0647\u06CC\u062F \u0632\u0645\u0627\u0646 \u0628\u0631 \u062D\u0633\u0628 GMT \u0628\u0627\u0634\u062F \u060C\u062F\u0631 \u062C\u0627\u06CC\u06CC \u06A9\u0647 \u062F\u0627\u062F\u0647 \u0647\u0627 \u0631\u0627 \u0648\u0627\u0631\u062F \u0645\u06CC \u06A9\u0646\u06CC\u062F \u062C\u0644\u0648\u06CC \n Ignore Time Zone \n\u062A\u06CC\u06A9 \u0628\u0632\u0646\u06CC\u062F. ", Item.PLAIN);
                // write post-init user code here
            }
            return stringItem10;
        }
        //</editor-fold>

        

        /**
         * Returns a display instance.
         * @return the display instance.
         */
        public Display getDisplay() {
            return Display.getDisplay(Oughat_Sharei_Shia.this);
        }

        /**
         * Exits MIDlet.
         */
        public void exitMIDlet() {
            switchDisplayable(null, null);
            destroyApp(true);
            notifyDestroyed();
        }

        /**
         * Called when MIDlet is started.
         * Checks whether the MIDlet have been already started and initialize/starts or resumes the MIDlet.
         */
        public void startApp() {

            if (f4) {
                resumeMIDlet();
            } else {
                initialize();
                startMIDlet();
            }
            f4 = false;
        }

        /**
         * Called when MIDlet is paused.
         */
        public void pauseApp() {
            f4 = true;
        }

        /**
         * Called to signal the MIDlet to terminate.
         * @param unconditional if true, then the MIDlet has to be unconditionally terminated and all resources has to be released.
         */
        public void destroyApp(boolean unconditional) {
        }

        private String readFromInput(DataInputStream dof) {
            C97 bs = new C97();
            while (true) {
                try {
                    bs.h(dof.readByte());
                } catch (Throwable ex) {
                    C17.m1(ex);
                    break;
                }
            }
            return new String(bs.nj());
        }

        private void m2() throws IOException {
            FileConnection f = this.altitudeFileBrowser.getSelectedFile();

            f.setReadable(true);

            InputStream is = f.openInputStream();

            this.f3.m1(is);

            is.close();
            f.close();
        }

        private void m4() throws IOException {
            final FileConnection file = this.locationsFileBrowser.getSelectedFile();
            final InputStream is = file.openInputStream();
            this.f5.m3(C21.m1(is));
            is.close();
            file.close();
        }

        private void m8() throws Exception {

            this.f8.ji(getChoiceGroup().isSelected(0));
            this.f8.f3(!getChoiceGroup().isSelected(1));

            if (this.getChoiceGroup1().isSelected(1)) {
                this.f2 = new C20("Created_Location", C99.o(Double.parseDouble(this.textField7.getString())),
                        C99.o(Double.parseDouble(this.textField8.getString())), C99.o(Double.parseDouble(this.textField9.getString())));

            } else {
                this.f2 = this.f5.m3(this.textField6.getString());
            }
//       if(this.currentLocation==null){
//           this.errorInputLocation = true;
//       };
            this.f8.m5(this.textField5.getString(), f2, f3);
            this.f7 = false;
            getChoiceGroup1().setSelectedFlags(new boolean[]{false, false});

            getShowingResultForm().setTitle(this.f2.m12() + " in: " + getTextField5().getString());
            try {
                setShowingStringItems();
            } catch (Throwable t) {
                C17.m1(t);
            }

        }

        private void setShowingStringItems() {
//        if(computer==null ||this.stringItem2==null){
////            Utils.println("d3eubdsbhuedybeybdyubdhdbijdiheduiheduehdu3heudh3eduhe3duh");
//            throw new RuntimeException("sdsdwdfwefefwfwfwwfwfww");
//        }
            this.getStringItem1().setText(this.f8.d32());
            this.getStringItem2().setText(this.f8.f44());
            this.getStringItem8().setText(this.f8.f32());
            this.getStringItem3().setText(this.f8.f900());
            this.getStringItem4().setText(this.f8.f123());
            this.getStringItem5().setText(this.f8.bey1());
            this.getStringItem6().setText(this.f8.wq23123());
            this.getStringItem7().setText(this.f8.r33());
        }

        private CC getShowingLocationsList() {
            return this.f9;
        }

        private void m3() {
            f3.m9(Double.parseDouble(textField.getString()));
            f3.m11(Double.parseDouble(getTextField2().getString()));
            f3.m12(Double.parseDouble(getTextField3().getString()));
            f3.m8(Double.parseDouble(getTextField1().getString()));
            f3.m6(Double.parseDouble(getTextField4().getString()));
            try {
                f3.m14(Double.parseDouble(getTextField11().getString()));
                f3.m13(false);
            } catch (Exception e) {
                if (getTextField11().getString().charAt(0) == 's' || getTextField11().getString().charAt(0) == 'S') {
                    f3.m14(Double.parseDouble(getTextField11().getString().substring(1)));
                    f3.m13(true);
                } else {
                    throw new IllegalArgumentException();
                }
            }
        }

        public class CC extends List implements CommandListener {

            private int startInd = 0;
            private int[] indexes;
            private boolean mode;//false-> uses startInd% true-> uses indexes
            private final Command selectCommand = new Command("Select", Command.OK, 0);
            private final Command cancelCommand = new Command("Cancel", Command.CANCEL, 2);

            public int getStartInd() {
                return startInd;
            }

            public CC() {
                super("Found locations:", List.IMPLICIT);
                this.addCommand(this.selectCommand);
                this.addCommand(this.cancelCommand);
                this.setCommandListener(this);
            }

            public void update(final C20[] locations, final int from, final int beforeIndex) {
                this.mode = false;
                this.deleteAll();
                this.startInd = from;
                for (int i = from; i < beforeIndex; i++) {
                    this.append(locations[i].m12(), null);
                }
            }

            public void update(final C20[] locations, final int[] indexes) {
                this.mode = true;
                this.indexes = indexes;
                this.deleteAll();
                for (int i = 0; i < indexes.length; i++) {
                    this.append(locations[indexes[i]].m12(), null);
                }
                final boolean[] temp = new boolean[indexes.length];
                for (int i = 0; i < temp.length; i++) {
                    temp[i] = false;
                }
                this.setSelectedFlags(temp);
            }

            public C20 getSelectedLocation() {
                if (mode) {
                    return C100.this.f5.m1(this.indexes[this.getSelectedIndex()]);
                } else {
                    return C100.this.f5.m1(this.startInd + this.getStartInd());
                }
            }

            public void commandAction(Command c, Displayable d) {
                if (c.getCommandType() == Command.CANCEL) {
                } else if (c.getCommandType() == Command.OK) {
                    C100.this.setCurrentLocation(this.getSelectedLocation());
                    C100.this.getTextField6().setString(C100.this.f2.m12());
                    C100.this.getTextField7().setString(Double.toString(C99.u(C100.this.f2.m13())));
                    C100.this.getTextField8().setString(Double.toString(C99.u(C100.this.f2.m14())));
                    C100.this.getTextField9().setString(Double.toString(C99.u(C100.this.f2.m15())));
                }
                C100.this.switchDisplayable(null, C100.this.getInputData());
            }
        }

        public void setCurrentLocation(C20 currentLocation) {
            this.f2 = currentLocation;
        }

        public C20 getCurrentLocation() {
            return f2;
        }
    }

    public static class C17 {

        

        public static void m1(final Throwable s) {
            s.printStackTrace();
        }

        public static String[] m2(final String in, final char i1) {
            final int sLength = in.length();
            int l1 = 0;
            int l2 = 0;
            int l3 = 0, l4 = in.length() - 1;
            for (int i = 0; i < sLength; i++) {
                if (i1 != in.charAt(i)) {
                    l3 = i;
                    break;
                }
            }
            for (int i = sLength - 1; i > -1; i--) {
                if (i1 != in.charAt(i)) {
                    l4 = i;
                    break;
                }
            }

            for (int i = l3; i < l4 + 1; i++) {
                if (in.charAt(i) == i1) {
                    l1++;
                }
            }
            String[] l5 = new String[l1 + 1];
            l1 = 0;
            String l6;
            int l7 = l3 - 1;

            for (int i = l3 + 1; i < l4 + 1; i++) {
                if (in.charAt(i) == i1) {
                    l5[l1] = in.substring(l7 + 1, i);
                    l1++;
                    l7 = i;
                }
            }
            l5[l1] = in.substring(l7 + 1, l4 + 1);
            return l5;
        }

        public static String m3(final DataInputStream dof) {
            C97 bs = new C97();
            while (true) {
                try {
                    bs.h(dof.readByte());
                } catch (Exception ex) {
                    break;
                }
            }
            return new String(bs.nj());
        }

        public static String inputStreamToString(InputStream is) throws IOException {
            final C97 byteSet = new C97();
            int temp = is.read();
            while (temp != -1) {
                byteSet.h((byte) temp);
//                System.out.println(new String(byteSet.toByteArray()));
                temp = is.read();
            }
            return new String(byteSet.nj());
        }

        public static StringBuffer deleteWideSpaces(final String in) {
            final StringBuffer buffer = new StringBuffer(in.length());
            char temp;
            for (int i = 0; i < in.length(); i++) {
                temp = in.charAt(i);
                if (!isWideSpace(temp)) {
                    buffer.append(temp);
                }
            }
            return buffer;
        }

        public static boolean isWideSpace(final char c) {
            if (c == ' ' || c == '\t' || c == '\n' || c == '\r') {
                return true;
            } else {
                return false;
            }
        }
    }

    public static class C21 {

        public static C20[] m1(InputStream is) throws IOException {

            final String l1 = C17.inputStreamToString(is);
//        Utils.println(inAsString);
            final String[] l2 = C17.m2(l1, '#');

            final C20[] l3 = new C20[l2.length];

            for (int i = 0; i < l2.length; i++) {
//            Utils.println("1-> "+lcationsAsString[i]);
                try {
                    l3[i] = new C20(l2[i]);
                } catch (RuntimeException re) {
                    l3[i] = new C20();
                }

            }
            return l3;
        }
    }

    public static class C19 {

        final private InputStream inputStream;
        private double f1[] = null;

        public C19(InputStream inputStream) {
            this.inputStream = inputStream;
        }

        public InputStream m1() {
            return inputStream;
        }

        public double[] m2() throws IOException {
            if (this.f1 == null) {

                final String s = C17.inputStreamToString(this.inputStream);
                final String[] l1 = C17.m2(s, ',');
                final double[] l2 = new double[l1.length];
                for (int i = 0; i < l2.length; i++) {
                    l2[i] = Double.parseDouble(l1[i]);
                }
//            inputStream.close();
                this.f1 = l2;
            }
            return this.f1;
        }
    }

    public static class C18 implements C45 {

        private double f1 = -4.4;
        private double f2 = -17.833;
        private double f3 = -17.833;
        private double f4 = -.833;
        private double f5 = -.833;
        private double f6 = 4.0 / 7.0;
        private boolean f7 = true;

        public boolean m15() {
            return f7;
        }

        public double m16() {
            return f6;
        }

        public void m13(boolean i1) {
            this.f7 = i1;
        }

        public void m14(double i2) {
            this.f6 = i2;
        }

        {
            try {
                C18.class.getResourceAsStream("altitudes.txt");

            } catch (Throwable t) {
                C17.m1(t);
            }
        }

        public void m1(final InputStream inputStream) throws IOException {
            double[] l1 = (new C19(inputStream)).m2();
            f2 = l1[0];
            f4 = l1[1];
            f5 = l1[2];
            f1 = l1[3];
            f3 = l1[4];

        }

        public void m6(final double i1) {
            this.f3 = i1;
        }

        public void m8(final double i1) {
            this.f1 = i1;
        }

        public void m9(final double i1) {
            this.f2 = i1;
        }

        public void m11(final double i1) {
            this.f4 = i1;
        }

        public void m12(final double i1) {
            this.f5 = i1;
        }

        public double m2(final double i1, final double i2) {
            if (this.f7) {
                final double l1, l2;
                l1 = C99.x(C99.s(i1) * C99.s(i2)
                        + C99.d(i1) * C99.d(i2));
                l2 = 1 / C99.m(l1) + this.f6;
                //Utils.println("zavie="<<atan(1/shadow_length);
                return C99.v(1 / l2);
            } else {
                return C99.v(1 / this.f6);
            }
        }

        public double m3() {
            return C99.o(f1);
        }

        public double m4() {
            return C99.o(f2);
//        new Long(value)
        }

        public double m5() {
            return C99.o(f3);
        }

        public double m6() {
            return C99.o(f4);
        }

        public double m7() {
            return C99.o(f5);
        }
    }

    public class C98 {

        private double r9;
        private double r1, r2, r3, r4, r5, r6, r7, r8;
        private C20 r10;
        private C45 r11;
        private boolean r12 = true;
        private boolean r13 = false;

        public void ji(boolean i1) {
            this.r13 = i1;
        }

        public boolean f2() {
            return r12;
        }

        public void f3(boolean in1) {
            this.r12 = in1;
        }

        public void m4(final double in11, final int in12, final C20 l, final C45 ac) {
            this.r9 = in11 + this.f45(in12);
            this.r10 = l;
            this.r11 = ac;
            t5();
        }

        public void m5(final String DMY, final C20 l, final C45 ac) {
            final String[] l1 = C17.m2(DMY, ' ');
            m4(m7(Integer.parseInt(l1[1]), Double.parseDouble(l1[0])), Integer.parseInt(l1[2]), l, ac);
        }

        /**
         *
         * @param t5
         * @param y5
         * @return
         * @throws IllegalArgumentException
         */
        private double m7(final int t5, final double y5) {
            int minus = 0;
            if (y5 >= 0 && y5 <= 33 && t5 > -1 && t5 < 13) {
                if (t5 > 7) {
                    minus = t5 - 7;
                }
            } else {
                throw new IllegalArgumentException();
            }
            return (y5 + 31 * (t5 - 1) - minus);
        }

        private double f45(int year) {
            int x = (year - 1387) % 4;
            if (x < 0) {
                x += 4;
            }
            //87 kabise
            return x / 4 * (2 * C99.WSS);
        }

        public String f89(final double i1) {
            final int ll1, ll2, ll3;
            final double temp = i1 / 2 / C99.WSS * 24;
            ll1 = (int) (temp);
            ll2 = (int) ((temp - ll1) * 60);

            ll3 = (int) ((temp - ll1 - (double) ll2 / 60.0) * 3600);
            return ll1 + " : " + ll2 + " : " + ll3;
        }

        public String f44() {
            return f89(this.r4);
        }

        public String f900() {
            return f89(this.r5);
        }

        public String f32() {
            return f89(this.r1);
        }

        public String f123() {
            return f89(this.r7);
        }

        public String bey1() {
            return f89(this.r2);
        }

        public String wq23123() {
            return f89(this.r3);
        }

        public String r33() {
            return f89(this.r6);
        }

        private double m2() {
            if (this.r12 && this.r9 > 0 && this.r9 < 187) {
                return C99.WSS / 12;
            } else {
                return 0;
            }
        }

        public double ew() {
            if (r13) {
                return (-r10.m14());
            } else {
                return (r10.m15() - r10.m14());
            }
        }

        public void t5() {
            final double l1 = C1.f2(r9) / 60 / 24 * 2 * C99.WSS;
            final double l2 = this.r10.m13();
            this.r5 = this.m2() + C99.WSS + this.ew() - l1;

            final double l3 = C1.c1f1(r9 + .25);
            this.r4 = this.r5 - C2.f5(this.r11.m4(), l2, l3);

            final double l4, l5 = C1.c1f1(this.r9 + .625);
            this.r7 = this.r5 + C2.f5(this.r11.m2(l2, l5), l2, l5);

            this.r1 = this.r5 - C2.f5(this.r11.m6(), l2, l3);

            final double l6 = C1.c1f1(this.r9 + .75);
            this.r2 = this.r5 + C2.f5(this.r11.m7(), l2, l6);

            this.r3 = this.r5 + C2.f5(this.r11.m3(), l2, l6);

            l4 = C1.c1f1(this.r9 + .825);
            this.r6 = this.r5 + C2.f5(this.r11.m5(), l2, l4);
            this.r8 = this.r5 + (11.25 * C99.WSS / 12);

            if (this.r5 < 0) {
                this.r5 += 2 * C99.WSS;
            } else if (this.r5 > 2 * C99.WSS) {
                this.r5 -= 2 * C99.WSS;
            }
            if (this.r7 < 0) {
                this.r7 += 2 * C99.WSS;
            } else if (this.r7 > 2 * C99.WSS) {
                this.r7 -= 2 * C99.WSS;
            }

            if (this.r3 < 0) {
                this.r3 += 2 * C99.WSS;
            } else if (this.r3 > 2 * C99.WSS) {
                this.r3 -= 2 * C99.WSS;
            }

            if (this.r2 < 0) {
                this.r2 += 2 * C99.WSS;
            } else if (this.r2 > 2 * C99.WSS) {
                this.r2 -= 2 * C99.WSS;
            }

            if (this.r6 < 0) {
                this.r6 += 2 * C99.WSS;
            } else if (this.r6 > 2 * C99.WSS) {
                this.r6 -= 2 * C99.WSS;
            }

            if (this.r1 < 0) {
                this.r1 += 2 * C99.WSS;
            } else if (this.r1 > 2 * C99.WSS) {
                this.r1 -= 2 * C99.WSS;
            }

            if (this.r4 < 0) {
                this.r4 += 2 * C99.WSS;
            } else if (this.r4 > 2 * C99.WSS) {
                this.r4 -= 2 * C99.WSS;
            }
        }

        public String d32() {
            return this.f89(r8);
        }
    }

    public static class C1 {
        //hegrie shamsi

        public static double c1f1(double in1) {
            final double l1 = c1f3(c1f2(in1));
            return 0.006918 - 0.399912 * C99.d(l1) + 0.070257 * C99.s(l1) - 0.006758
                    * C99.d(2 * l1) + 0.000907 * C99.s(l1 * 2) - 0.002697
                    * C99.d(3 * l1) + 0.00148 * C99.s(3 * l1);
        }

        private static double c1f2(double in1) {
            if (in1 >= 286) {
                in1 -= 285;
            } else {
                in1 += 80;
            }
            return in1;
        }

        public static double c1f3(double in1) {
            return (2 * C99.WSS * (in1 - 1.5) / 365.25);
        }

        public static double f2(double in1) {
            final double gama = c1f3(c1f2(in1));
            return 229.18 * (0.000075 + 0.001868 * C99.d(gama) - 0.032077 * C99.s(gama)
                    - 0.014615 * C99.d(2 * gama) - 0.040849 * C99.s(2 * gama));
        }
    }

    public static class C2 {

        public static double f1(double in1, double in2, double in3) {
            return C99.x(Math.sin(in1) * C99.s(in2) + C99.d(in1) * C99.d(in2) * C99.d(in3));
        }

        public static double f2(double in1, double in2) {
            double ll1, ll2;
            ll1 = C99.x(C99.s(in1) * C99.s(in2) + C99.d(in1) * C99.d(in2));
            ll2 = 1 / Math.tan(ll1) + 4.0 / 7.0;
            //Utils.println("zavie="<<atan(1/shadow_length);
            return C99.v(1 / ll2);
        }

        public static double f5(double i1, double i2, double i3) {
            return C99.a((C99.s(i1) - C99.s(i2) * C99.s(i3))
                    / (C99.d(i2) * C99.d(i3)));
        }
    }

    public static class C12 {

        private final static C45 INSTANCE_ = new C18();

        public static C45 getAltitudeCalculator() {
            return INSTANCE_;
        }
    }

    public static class C20 {

        private final String f1;
        private final double f2;
        private final double f3;
        private final double f4;

        public C20() {
            this("No_Name", 0, 0, 0);
        }

        public boolean m1(C20 loc) {

            return this.m12().equals(loc.m12());
        }

        public C20(String i1, double i2, double i3,
                double i4) {
            super();
            this.f1 = i1;
            this.f2 = i2;
            this.f3 = i3;
            this.f4 = i4;
        }

        public C20(String i1) {
            i1 = C17.deleteWideSpaces(i1).toString();
            final String[] amoAsStr = C17.m2(i1, ',');
            final double[] amounts = new double[amoAsStr.length - 1];
            for (int i = 0; i < amounts.length; i++) {
                amounts[i] = Double.parseDouble(amoAsStr[i + 1]);
            }
//        this(amoAsStr[0],MyMath.toRadian(amounts[0]),MyMath.toRadian(amounts[1]),MyMath.toRadian(amounts[2]));
            this.f1 = amoAsStr[0];
            this.f2 = C99.o(amounts[0]);
            this.f3 = C99.o(amounts[1]);
            this.f4 = C99.o(amounts[2]);
//        Utils.print("\n\n>>>>>>>>>>>>>>>>>>>>>>>> "+this.toString()+" <<<<<<<<<<<<<<<\n\n");
        }

        public String m12() {
            return f1;
        }

        public double m13() {
            return f2;
        }

        public double m14() {
            return f3;
        }

        public double m15() {
            return f4;
        }

    }

    public static class C11 {

        private C20[] f1;
        private static final C20[] fs1 = ms1();

        private static C20[] ms1() {
            final C20[] temp = new C20[5];
            temp[0] = new C20("Mashhad", C99.o(36.31), C99.o(59.6333), C99.o(52.5));
            temp[1] = new C20("Tehran", C99.o(35.6833), C99.o(51.41666), C99.o(52.5));
            temp[2] = new C20("Shiraz", C99.o(29.616666), C99.o(52.55), C99.o(52.5));
            temp[3] = new C20("Tabriz", C99.o(38.05), C99.o(46.28), C99.o(52.5));
            temp[4] = new C20("Esfahan", C99.o(32.68), C99.o(51.64), C99.o(52.5));

            return temp;
        }

        {
            try {
                this.f1 = C21.m1(getClass().getResourceAsStream("locations.txt"));
            } catch (Throwable e) {
//            Utils.print(e);
//            Utils.println();
                this.f1 = C11.fs1;
            }
        }

        public C20 m1(final int i1) {
            return this.f1[i1];
        }

        public C20[] m2() {
            return f1;
        }

        public void m3(C20[] i) {
            this.f1 = i;
        }

        public C20 m3(String m) throws Exception {
            for (int i = 0; i < this.f1.length; i++) {
                if (this.f1[i].m12().equalsIgnoreCase(m)) {
                    return this.f1[i];
                }
            }
            throw new Exception("Location not found.");
        }

        public int[] m4(String i1) {
            C25 l1 = new C25();
            for (int i = 0; i < m2().length; i++) {
                if (m1(i).m12().toLowerCase().startsWith(i1.toLowerCase())) {
                    l1.m2(new Integer(i));
                }
            }
            int[] l2 = new int[l1.m2()];
            for (int i = 0; i < l2.length; i++) {
                l2[i] = ((Integer) (l1.m1(i))).intValue();
            }
            return l2;
        }

        public C11() {
//        ObjectInputStream
        }
    }

   

    public static class C25 {

        private boolean f1;
        private Object[] f2;
        private int f3 = 0;
        private int f4;

        public Object m1(int i1) {
            if (i1 < m2() && i1 > -1) {
                return f2[i1];
            } else {
                throw new IllegalArgumentException();
            }
        }

        public C25(boolean i1
                ) {
            this(16, i1);

        }

        public C25() {
            this(true);
        }

        public C25(int i1) {
            this(i1, true);
        }

        public C25(int i2, boolean i1) {
            this.f4 = i2;
            f2 = new Object[i2];
            this.f1 = i1;
        }

        public void m2(final Object in) {
//            Utils.println(size);
            if (!f1 && this.m4(in)) {
                return;
            }
            if (f3 >= f4) {
                f4 *= 2;
                final Object[] l1 = new Object[f2.length];
                for (int i = 0; i < f2.length; i++) {
                    l1[i] = f2[i];
                }
//                        Utils.println(capacity);
                f2 = new Object[f4];
                for (int i = 0; i < l1.length; i++) {
                    f2[i] = l1[i];
                }
            }

            f2[f3] = in;
            f3++;

        }

        public int m2() {
            return f3;
        }

        public Object[] m3() {
            final Object[] res = new Object[this.m2()];
            for (int i = 0; i < res.length; i++) {
                res[i] = this.m1(i);
            }
            return res;
        }

        public boolean m4(Object i1) {
            for (int i = 0; i < m2(); i++) {
                if (f2[i].equals(i1)) {
                    return true;
                }
            }
            return false;
        }
    }

    public interface C45 {

        public void m1(final InputStream inputStream) throws IOException;

        public double m2(final double latitude, final double declination);

        public double m3();

        public double m4();

        public double m5();

        public double m6();

        public double m7();

        public void m6(final double i1);

        public void m8(final double i1);

        public void m9(final double i1);

        public void m11(final double i1);

        public void m12(final double i2);

        public void m13(boolean i1);

        public void m14(double i1);

        public boolean m15();

        public double m16();
    }

    public static class C97 {

        private boolean n;
        private byte[] d;
        private int p = 0;
        private int u;

        public byte y(int i1) {
            if (i1 > -1 && i1 < k()) {
                return d[i1];
            } else {
                throw new IllegalArgumentException("Index is greater than size of set or is negative.");
            }
        }

        public C97(boolean i1) {
            this(16, i1);

        }

        public C97() {
            this(true);
        }

        public C97(int i1) {
            this(i1, true);
        }

        public C97(int i, boolean u1) {
            this.u = i;
            d = new byte[i];
            this.n = u1;
        }

        public void h(byte in) {
//            Utils.println(size);
            if (!n && this.fj(in)) {
                return;
            }
            if (p >= u) {
                u *= 2;
                final byte[] l1 = new byte[d.length];
                for (int i = 0; i < d.length; i++) {
                    l1[i] = d[i];
                }
//                        Utils.println(capacity);
                d = new byte[u];
                for (int i = 0; i < l1.length; i++) {
                    d[i] = l1[i];
                }
            }

            d[p] = in;
            p++;

        }

        public int k() {
            return p;
        }

        public byte[] nj() {
            final byte[] l1 = new byte[this.k()];
            for (int i = 0; i < l1.length; i++) {
                l1[i] = this.y(i);
            }
            return l1;
        }

        public boolean fj(byte i1) {
            for (int i = 0; i < k(); i++) {
                if (i1 == d[i]) {
                    return true;
                }
            }
            return false;
        }
    }
    public static class C99 {

    private C99(){

    }

    public static double u(double i1){
        return Math.toDegrees(i1);
    }
    public static double o(double i2){
        return Math.toRadians(i2);
    }
    public static double y(final double x,final int n){
        double l1  =1;
        if(n>0){
        for (int i = 0; i <n; i++) {
            l1*=x;
        }
        }else if(n<0){
            for (int i = 0; i <-n; i++) {
            l1/=x;
        }
        }
        return l1;
    }
    public static double t(final double arg){
        return Math.sqrt(arg);
    }
    public final static double WSS = Math.PI;
    public static double d(final double arg){
        return Math.cos(arg);
    }
    public static double s(final double arg){
        return Math.sin(arg);
    }


    public static double a(double arg){
        if(arg==1){
            return 0;
        }else if(arg==-1){
            return WSS;
        }else if(arg==0){
            return WSS/2;
        }
        return z(0, C99.WSS, arg);
    }
    private static double z(double unner,double upper,double arg){
        final double midAngle = (upper+unner)/2;
       final double midArc =  C99.d(midAngle);
       try{
       if(arg==midArc){
           return midAngle;
       }else if(arg<midArc){
           return z(midAngle, upper, arg);
       }else {
          return  z(unner,midAngle, arg);
       }
       }catch(Throwable t){
           return midAngle;
       }
    }
    public static double x(double arg){
        if(arg==1){
            return WSS/2;
        }else if(arg==-1){
            return -WSS/2;
        }else if(arg==0){
            return 0;
        }
        return c(-C99.WSS/2, C99.WSS/2, arg);
    }
    private static double c(double unner,double upper,double arg){
        final double midAngle = (upper+unner)/2;
       final double midArc =  C99.s(midAngle);
       try{
       if(arg==midArc){
           return midAngle;
       }else if(arg>midArc){
           return c(midAngle, upper, arg);
       }else {
          return  c(unner,midAngle, arg);
       }
       }catch(Throwable t){
           return midAngle;
       }
    }
    public static double v(double arg){
        if(arg==1){
            return WSS/4;
        }else if(arg==-1){
            return -WSS/4;
        }else if(arg==0){
            return 0;
        }
        return n(-C99.WSS/2, C99.WSS/2, arg);
    }
    private static double n(double unner,double upper,double arg){
        final double midAngle = (upper+unner)/2;
       final double midArc =  C99.m(midAngle);
       try{
       if(arg==midArc){
           return midAngle;
       }else if(arg>midArc){
           return n(midAngle, upper, arg);
       }else {
          return  n(unner,midAngle, arg);
       }
       }catch(Throwable t){
           return midAngle;
       }
    }

    public static double m(double arg){
        return Math.tan(arg);
    }
    }

}
