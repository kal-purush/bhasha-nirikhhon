package GUI;


import Model.LawnMower;
import Model.SimulationMonitor;
import Viewer.MowerStatus;

import javax.swing.*;
import java.awt.*;

public class MainPanel extends JFrame {

    public MainPanel(SimulationMonitor simulationMonitor) {
        this.simulationMonitor = simulationMonitor;
        initComponents();
    }

    private void initComponents() {

        btnPanel = new JPanel();
        nextBtn = new JButton();
        stopBtn = new JButton();
        resetBtn = new JButton();
        forwardBtn = new JButton();
        txtPanel1 = new JPanel();
        currentState = new java.awt.Label();
        txtPanel2 = new JPanel();
        txtPanel3 = new JPanel();
        cutGrassState = new java.awt.Label();
        remainGrassState = new java.awt.Label();
        mowerStatusPanel = new JScrollPane();
        currentLawn = new JTable();
        realTimeLawnPanel = new JScrollPane();
        currentLawnPanel = new LawnPanel(500, 500, simulationMonitor.getLawn().getWidth(), simulationMonitor.getLawn().getHeight(), simulationMonitor.getLawn(), 2, 20, 12);
        jLabel1 = new JLabel();

        setDefaultCloseOperation(WindowConstants.EXIT_ON_CLOSE);

        nextBtn.setText("Next");
        nextBtn.setActionCommand("");
        nextBtn.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                nextBtnActionPerformed(evt);
            }
        });

        stopBtn.setText("Stop");
        stopBtn.setActionCommand("");
        stopBtn.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                stopBtnActionPerformed(evt);
            }
        });

        forwardBtn.setText("Fast-Forward");
        forwardBtn.setActionCommand("");
        forwardBtn.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                forwardBtnActionPerformed(evt);
            }
        });

        resetBtn.setText("Reset");
        resetBtn.setActionCommand("");
        resetBtn.addActionListener(new java.awt.event.ActionListener() {
            public void actionPerformed(java.awt.event.ActionEvent evt) {
                resetBtnActionPerformed(evt);
            }
        });


        GroupLayout btnPanelLayout = new GroupLayout(btnPanel);
        btnPanel.setLayout(btnPanelLayout);
        btnPanelLayout.setHorizontalGroup(
                btnPanelLayout.createParallelGroup(GroupLayout.Alignment.CENTER)
                        .addGroup(btnPanelLayout.createSequentialGroup()
                                .addGap(20, 25, 40)
                                .addComponent(nextBtn)
                                .addGap(20, 25, 40)
                                .addComponent(stopBtn)
                                .addGap(20, 25, 40)
                                .addComponent(forwardBtn)
                                .addGap(20, 25, 40)
                                .addComponent(resetBtn))
        );
        btnPanelLayout.setVerticalGroup(
                btnPanelLayout.createParallelGroup(GroupLayout.Alignment.CENTER)
                        .addGroup(btnPanelLayout.createSequentialGroup()
                                .addGap(11, 11, 11)
                                .addGroup(btnPanelLayout.createParallelGroup(GroupLayout.Alignment.LEADING)
                                        .addComponent(nextBtn)
                                        .addComponent(stopBtn)
                                        .addComponent(resetBtn)
                                        .addComponent(forwardBtn)))
        );

        currentState.setText("Turns Done: ");

        generateTxtPanelLayout(txtPanel1, currentState);

        cutGrassState.setText("Grass Cut: ");

        generateTxtPanelLayout(txtPanel2, cutGrassState);

        remainGrassState.setText("Grass Remaining: ");

        generateTxtPanelLayout(txtPanel3, remainGrassState);


        //====================
        // mower status
        // TODO: add an update function to update the Mower status

        String[] columnNames = new String[]{
                "Mower ID", "Current Status", "Current Direction", "Next Action"
        };

        int mowerCount = simulationMonitor.getMowerList().length;
        String[][] data = new String[mowerCount][4];

        //initialize the mower status table

        for (int i = 0; i < mowerCount; i++) {
            data[i][0] = "Mower " + (i + 1);
        }
        for (int i = 0; i < mowerCount; i++) {
            data[i][1] = "N/A";
            data[i][2] = "North";
            data[i][3] = "Turned Off";
        }

        JTable mowerStatusTable = new JTable(data, columnNames);
        mowerStatusPanel.setViewportView(mowerStatusTable);

//        currentLawn.setModel(new table.DefaultTableModel(
//                new Object[][]{
//                        {"", null, null, null},
//                        {null, null, null, null},
//                        {null, null, null, null},
//                        {null, null, null, null}
//                },
//                new String[]{
//                        "Mower ID", "Current Status", "Current Direction", "Next Action"
//                }
//        ));
//        currentLawn.setAutoscrolls(false);
//        currentLawn.setRowHeight(24);
//        mowerStatusPane1.setViewportView(currentLawn);
//        currentLawn.getAccessibleContext().setAccessibleParent(currentLawn);


        // mower status
        //====================

        currentLawnPanel.setAutoscrolls(false);
        realTimeLawnPanel.setViewportView(currentLawnPanel);

        jLabel1.setText("Mower State");

        GroupLayout layout = new GroupLayout(getContentPane());
        getContentPane().setLayout(layout);
        layout.setHorizontalGroup(
                layout.createParallelGroup(GroupLayout.Alignment.CENTER)
                        .addGroup(GroupLayout.Alignment.CENTER, layout.createSequentialGroup()
                                .addComponent(btnPanel, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
                        .addGroup(GroupLayout.Alignment.CENTER, layout.createSequentialGroup()
                                .addComponent(txtPanel3, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                                .addComponent(txtPanel2, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                                .addComponent(txtPanel1, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
                        .addGroup(GroupLayout.Alignment.CENTER, layout.createSequentialGroup()
                                .addComponent(jLabel1))
                        .addGroup(GroupLayout.Alignment.CENTER, layout.createSequentialGroup()
                                .addComponent(mowerStatusPanel, GroupLayout.PREFERRED_SIZE, 450, GroupLayout.PREFERRED_SIZE))
                        .addGroup(GroupLayout.Alignment.CENTER, layout.createSequentialGroup()
                                .addComponent(realTimeLawnPanel, GroupLayout.PREFERRED_SIZE, 500, GroupLayout.PREFERRED_SIZE))
        );
        layout.setVerticalGroup(
                layout.createParallelGroup(GroupLayout.Alignment.LEADING)
                        .addGroup(layout.createSequentialGroup()
                                .addContainerGap()
                                .addComponent(btnPanel, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                                .addGap(30, 30, 30)
                                .addComponent(jLabel1)
                                .addGap(30, 30, 30)
                                .addComponent(mowerStatusPanel, GroupLayout.PREFERRED_SIZE, 200, GroupLayout.PREFERRED_SIZE)
                                .addGap(30, 30, 30)
                                .addGroup(layout.createParallelGroup(GroupLayout.Alignment.LEADING)
                                        .addComponent(txtPanel3, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                                        .addComponent(txtPanel2, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                                        .addComponent(txtPanel1, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE))
                                .addGap(30, 30, 30)
                                .addComponent(realTimeLawnPanel, GroupLayout.PREFERRED_SIZE, 500, GroupLayout.PREFERRED_SIZE)
                                .addContainerGap(GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
        );

        pack();
    }

    private void generateTxtPanelLayout(JPanel txtPanel, Label state) {
        GroupLayout txtPanelLayout = new GroupLayout(txtPanel);
        txtPanel.setLayout(txtPanelLayout);
        txtPanelLayout.setHorizontalGroup(
                txtPanelLayout.createParallelGroup(GroupLayout.Alignment.LEADING)
                        .addGroup(txtPanelLayout.createSequentialGroup()
                                .addContainerGap()
                                .addComponent(state, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                                .addContainerGap(GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
        );
        txtPanelLayout.setVerticalGroup(
                txtPanelLayout.createParallelGroup(GroupLayout.Alignment.LEADING)
                        .addGroup(txtPanelLayout.createSequentialGroup()
                                .addContainerGap()
                                .addComponent(state, GroupLayout.PREFERRED_SIZE, GroupLayout.DEFAULT_SIZE, GroupLayout.PREFERRED_SIZE)
                                .addContainerGap(GroupLayout.DEFAULT_SIZE, Short.MAX_VALUE))
        );
    }

    private void stopBtnActionPerformed(java.awt.event.ActionEvent evt) {
        LawnMower[] mowerList = simulationMonitor.getMowerList();
        for (int i = 0; i < mowerList.length; i++) {
            mowerList[i].setCurrentStatus(MowerStatus.turnedOff);
        }
        // TODO: get an alert window to say Simulation stopped?
    }

    private void forwardBtnActionPerformed(java.awt.event.ActionEvent evt) {
        while (simulationMonitor.issimulationOn()) {
            simulationMonitor.nextMove();
            currentLawnPanel.update(simulationMonitor.getLawn().getWidth(), simulationMonitor.getLawn().getHeight(), simulationMonitor.getLawn());
            cutGrassState.setText("# grass cut  " + simulationMonitor.getCutGrass() + "\n   # grass remaining");
        }
    }

    private void nextBtnActionPerformed(java.awt.event.ActionEvent evt) {
        simulationMonitor.nextMove();
        currentLawnPanel.update(simulationMonitor.getLawn().getWidth(), simulationMonitor.getLawn().getHeight(), simulationMonitor.getLawn());
        cutGrassState.setText("# grass cut  " + simulationMonitor.getCutGrass() + "\n   # grass remaining");
    }

    private void resetBtnActionPerformed(java.awt.event.ActionEvent evt) {
//        simulationMonitor.reset();
//        currentLawnPanel.update(simulationMonitor.getLawn().getWidth(), simulationMonitor.getLawn().getHeight(), simulationMonitor.getInitialLawn());
    }

    private void updateGUI() {

    }


    // Variables declaration - do not modify
    private JPanel btnPanel;
    private JTable currentLawn;
    private LawnPanel currentLawnPanel;
    private java.awt.Label currentState;
    private JButton forwardBtn;
    private java.awt.Label cutGrassState;
    private java.awt.Label remainGrassState;
    private JLabel jLabel1;
    private JScrollPane mowerStatusPanel;
    private JScrollPane realTimeLawnPanel;
    private JButton nextBtn;
    private JButton stopBtn;
    private JButton resetBtn;
    private JPanel txtPanel1;
    private JPanel txtPanel2;
    private JPanel txtPanel3;
    // End of variables declaration
    private SimulationMonitor simulationMonitor;
}