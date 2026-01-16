package HW8;

import HW7.Map;

import javax.swing.*;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

public class HomeWork8 extends JFrame {

    private int width = 800;
    private int height = 600;
    private int posX = 300;
    private int posY = 75;

    private JButton btnStart;
    private JButton btnExit;
    private JButton btnClearLog;

    private final int MIN_SIZE_MAP = 3;
    private final int MAX_SIZE_MAP = 10;
    private final int MIN_WIN_LENGHT = 3;

    private JPanel mainToolsPanel;
    private JPanel settingsPanel;

    private JSlider setupMapSize;
    private JLabel labelMapSize;

    private JSlider setupWinLenght;
    private JLabel labelWinLenght;

    private static JTextArea log;
    private  JScrollPane scrollLog;

    private Map map;

    HomeWork8() {

        prepareWindow();
        prepareToolsPanel();
        prepareButtons();
        prepareSettingsPanel();
        prepareGamelog();


        map = new Map();

        mainToolsPanel.add(settingsPanel,BorderLayout.NORTH);
        mainToolsPanel.add(scrollLog, BorderLayout.SOUTH);

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
        mainToolsPanel.setLayout(new GridLayout(2,1));
    }

    private void prepareButtons() {
    btnStart = new JButton("Start game");
    btnStart.addActionListener(new ActionListener() {
        @Override
        public void actionPerformed(ActionEvent e) {
            collectSettings();
        }
    });

    btnExit = new JButton("End game");
        btnExit.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                System.exit(0);

            }
        });


    btnClearLog = new JButton("Clear ");
        btnClearLog.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                log.setText("");

            }
        });
    }

    private void prepareSettingsPanel() {
        settingsPanel = new JPanel();
        mainToolsPanel.setBackground(Color.BLUE);
        settingsPanel.setLayout(new GridLayout(7,1));

        labelMapSize = new JLabel("Map Size " + MIN_SIZE_MAP + "x" + MIN_SIZE_MAP);
        setupMapSize = new JSlider(MIN_SIZE_MAP, MAX_SIZE_MAP,MIN_SIZE_MAP);
        setupMapSize.addChangeListener(new ChangeListener() {
            @Override
            public void stateChanged(ChangeEvent e) {
                int currentPosition = setupMapSize.getValue();
                labelMapSize.setText("Map Size " + currentPosition + "x" + currentPosition);
                setupWinLenght.setMaximum(currentPosition);
                sendToLog("Hello " + currentPosition);
            }
        });

        labelWinLenght = new JLabel("Win Lenght " + MIN_WIN_LENGHT);
        setupWinLenght = new JSlider(MIN_WIN_LENGHT, MIN_SIZE_MAP, MIN_WIN_LENGHT);
        setupWinLenght.addChangeListener(new ChangeListener() {
            @Override
            public void stateChanged(ChangeEvent e) {
                labelWinLenght.setText("Win Lenght " + setupWinLenght.getValue());
            }
        });

        settingsPanel.add(btnStart);
        settingsPanel.add(btnExit);
        settingsPanel.add(btnClearLog);
        settingsPanel.add(labelMapSize);
        settingsPanel.add(setupMapSize);
        settingsPanel.add(labelWinLenght);
        settingsPanel.add(setupWinLenght);

    }

    private void prepareGamelog() {
        log = new JTextArea();
        scrollLog = new JScrollPane(log);
        log.setEditable(false);
        log.setLineWrap(true);
    }

    static void sendToLog(String text) {
        log.append(text + "\n");
    }

    private void collectSettings() {
        int mapSize = setupMapSize.getValue();
        int winLen = setupWinLenght.getValue();
        log.setText("");
        sendToLog(" ===  Start New Game  ===");
        sendToLog("Map Size is" + mapSize + "x" + mapSize);
        sendToLog("Win Lenght is " + winLen);
        map.startGame(mapSize, mapSize,winLen);
    }


}