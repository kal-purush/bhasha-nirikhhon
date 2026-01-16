package service;

import java.awt.*;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

public class ScreenData {

    private List<Screen> connectedScreens = new ArrayList<>();
    private Screen presentationScreen;
    private Screen mainScreen;

    public ScreenData(){
        reloadConnectedScreens();
    }

    /**
     * load/reload amount and size of the connected Screens
     */
    public void reloadConnectedScreens(){
        connectedScreens.clear();

        try {
            GraphicsDevice[] gd = GraphicsEnvironment.getLocalGraphicsEnvironment().getScreenDevices();

            for(var screen : gd){
                Screen currentScreen = new Screen(screen.getDisplayMode().getWidth(), screen.getDisplayMode().getHeight(), screen.getIDstring());
                connectedScreens.add(currentScreen);
                if(GraphicsEnvironment.getLocalGraphicsEnvironment().getDefaultScreenDevice().equals(screen)) {
                    mainScreen = currentScreen;
                }
            }

            //if more than one screen available take the first screen which isn't the mainscreen as presentationscreen
            if(connectedScreens.size() > 1){
                presentationScreen = connectedScreens.stream().filter(screen -> screen != mainScreen).collect(Collectors.toList()).get(0);
            }

        } catch (HeadlessException e) {//throws in Environments without displaysupport
            e.printStackTrace();
        }
    }

    /**
     * get the Mainscreen
     * @return screen for mainmenue, editing, etc.
     */
    public Screen getMainScreen(){
        return mainScreen;
    }

    /**
     * get the Screen for the presentationmode
     * @return Screen for presentation
     */
    public Screen getPresentationScreen(){
        return presentationScreen;
    }

    /**
     * get all Screendevices
     * @return all connected Screendevices
     */
    public List<Screen> getConnectedScreens(){
        return connectedScreens;
    }

    /**
     * Data of a single Screen
     */
    public class Screen{
        private int width;
        private int height;
        private String ID;

        protected Screen(int width, int height, String ID){
            this.width = width;
            this.height = height;
            this.ID = ID;
        }

        public String toString(){
            return ID;
        }
    }
}