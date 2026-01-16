package Framework.UISystem;

import Setup.Main;

public class TimedWindow extends Window {
    boolean show = false;
    int time = 0;
    int timeShown;
    TimedWindow(int getX, int getY, int getSizeX, int getSizeY, String getName, int getTimeShown) {
      super(getX, getY, getSizeX, getSizeY, getName);
      timeShown = getTimeShown;
    }
  
    public void show() {
      time = Main.main.millis();
      show = true;
    }
  
    public void stepWindow() {
      if (show) {  
        if (Main.main.millis() > time + timeShown) {
          show = false;
        } else {
          super.stepWindow();
        }
      }
    }
  
    public void drawWindow() {
      if (show) {
        super.drawWindow();
      }
    }
}