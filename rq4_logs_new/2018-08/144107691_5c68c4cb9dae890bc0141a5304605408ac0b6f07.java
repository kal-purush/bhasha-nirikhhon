import javax.swing.*;
import java.awt.*;
import java.util.concurrent.TimeUnit;

public class Alert extends Thread{
    private boolean bottom;

    private void showAlert(String deviceName) {
        JOptionPane alert = new JOptionPane();
        String alertMessage = "You are losing your " + deviceName + "!!!";
        JOptionPane.showMessageDialog(alert,  alertMessage, "Warning!",JOptionPane.WARNING_MESSAGE);
    }

    private void listenChange() {
        while(bottom) {
            try {
                sleep(5000);
            } catch (InterruptedException i) {}
            showAlert("haha");
        }
    }

    public Alert() {
        bottom = true;
    }

    public void run() {
        listenChange();
    }

    void setBottom(boolean status) {
        bottom = status;
    }
}