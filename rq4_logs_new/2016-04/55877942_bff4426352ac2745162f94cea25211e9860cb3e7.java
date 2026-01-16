package com.Brendon;

import javax.swing.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Calendar;
import java.util.Date;

/*
This is the widow for entering a job for Air Conditioning, it will pass the information
back to the main window and add the job to the AC list and display the job.
 */
public class AC_GUI extends JFrame {
    private JPanel ACPanel;
    private JTextField AddressEntry;
    private JTextField IssueEntry;
    private JComboBox ACSelection;
    private JButton addToListButton;
    private Date date = Calendar.getInstance().getTime();


    AC_GUI(final HVAC_MainGUI parentComponent) {

        setContentPane(ACPanel);
        setLocation(300,300);
        pack();
        setVisible(true);
        parentComponent.setEnabled(false);

        ACSelection.addItem("Split System");
        ACSelection.addItem("Heat Pump");
        ACSelection.addItem("Central Air");
        ACSelection.addItem("Ductless System");

        setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);

        addToListButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {

                String address = AddressEntry.getText();
                String issue = IssueEntry.getText();
                String type = (String) ACSelection.getSelectedItem();

                jobAC acJob = new jobAC(address,issue,type,date);

                parentComponent.openList.add(acJob);
                parentComponent.openTickets.addElement(acJob);
                parentComponent.setEnabled(true);
                AC_GUI.this.dispose();

            }
        });


    }
}