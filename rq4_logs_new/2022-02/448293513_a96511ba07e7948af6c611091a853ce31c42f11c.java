package HW7;

import javax.swing.*;
import java.awt.*;

public class HomeWork7 extends JFrame {

    private int width = 800;
    private int height = 600;
    private int posX = 300;
    private int posY = 75;

    private JButton btnStart;
    private JButton btnExit;

    private JPanel mainToolsPanel;
    private JPanel settingsPanel;

    private Map map;

    HomeWork7() {

        prepareWindow();
        prepareToolsPanel();
        prepareButtons();
        prepareSettingsPanel();


        map = new Map();

        mainToolsPanel.add(settingsPanel,BorderLayout.NORTH);

        add(mainToolsPanel,BorderLayout.EAST);

        add(map);

         setVisible(true);

    }

    private void prepareWindow() {
        setSize(width,height);
        setLocation(posX, posY);
        setTitle("HomeWork7");
        setResizable(false);
        setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);
    }
    private void prepareToolsPanel() {
        mainToolsPanel = new JPanel();
        mainToolsPanel.setBackground(Color.ORANGE);
        mainToolsPanel.setLayout(new GridLayout(2,1));
    }

    private void prepareButtons() {
    btnStart = new JButton("Start game");
    btnExit = new JButton("End game");
    }
    private void prepareSettingsPanel() {
        settingsPanel = new JPanel();
        mainToolsPanel.setBackground(Color.BLUE);
        settingsPanel.setLayout(new GridLayout(4,1));

        settingsPanel.add(btnStart);
        settingsPanel.add(btnExit);

    }


}