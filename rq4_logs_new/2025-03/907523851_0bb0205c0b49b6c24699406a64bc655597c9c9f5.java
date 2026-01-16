package src;

import java.io.DataOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.text.DecimalFormat;

import javax.swing.JLabel;
import javax.swing.JOptionPane;

public class FileLoader implements Runnable {

    private byte[] fileData;
    private String filePath;
    private MainFrame mainFrame;
    private JLabel statusBarLabel;

    public FileLoader(String filePath, byte[] fileData, MainFrame mainFrame, JLabel statusBarLabel) {
        this.fileData = fileData;
        this.filePath = filePath;
        this.statusBarLabel = statusBarLabel;
    }

    @Override
    public void run() {
        DecimalFormat df = new DecimalFormat("#.#");
        statusBarLabel.setText("Saving " + filePath.split("/")[filePath.split("/").length-1] + " [" + df.format((double) fileData.length / 1024.0) + " KiB]...");
        DataOutputStream out;
        try {
            out = new DataOutputStream(new FileOutputStream(filePath));
            out.write(fileData);
            out.close();
        } catch (FileNotFoundException e) {
            JOptionPane.showMessageDialog(mainFrame,
                    "Не удалось скачать файл.",
                    e.getMessage(), JOptionPane.ERROR_MESSAGE);
            e.printStackTrace();
        } catch (IOException e) {
            JOptionPane.showMessageDialog(mainFrame,
                    "Не удалось скачать файл.",
                    e.getMessage(), JOptionPane.ERROR_MESSAGE);
            e.printStackTrace();
        }
        statusBarLabel.setText("");
    }
    
    
}