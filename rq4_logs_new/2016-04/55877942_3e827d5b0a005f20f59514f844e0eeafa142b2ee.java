package com.Brendon;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.Date;
import java.util.Vector;


public class FurnaceGUI extends JFrame{
    private JPanel rootPanel;
    private JTextField addressEntry;
    private JLabel AddressEntry;
    private JTextField issueEntry;
    private JComboBox typeSelection;
    private JButton closeButton;
    private Date date;

    HVAC_MainGUI hvacManager = new HVAC_MainGUI();



    public void FurnaceGUI() {


        setContentPane(rootPanel);
        pack();
        setVisible(true);

        hvacManager.data = new Vector<Job>();


        typeSelection.addItem("Forced Air");
        typeSelection.addItem("Boiler");
        typeSelection.addItem("Octopus Style");

        setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);

        closeButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {

                String address = addressEntry.getText();
                String issue = issueEntry.getText();
                String type = (String) typeSelection.getSelectedItem();


                Job furn = new Job(address,issue,type,date);

                hvacManager.data.add(furn);

                for (Job t : hvacManager.data){ // trying to display this information on the previous windoe

                    hvacManager.tickets.addElement(t);
                }

                setVisible(false);


            }
        });

    }




}