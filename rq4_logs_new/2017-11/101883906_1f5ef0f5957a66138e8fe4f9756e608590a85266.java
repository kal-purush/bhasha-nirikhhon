package com;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

/*
 * Created by Aniruddh on 01/11/2017.
 */

public class Main {

    // Create references
    JFrame f;
    int count_clicks = 0;

    // Create a constructor to set the GUI
    Main(){
        // Allocate memory to frame
        f = new JFrame();

        // Create Button and Label
        JButton b1 = new JButton("Click Here");
        JLabel l1 = new JLabel("No of clicks");

        // Set default count to label
        l1.setText(String.valueOf(count_clicks));

        // Add action listener to increase count whenever the button is clicked
        b1.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                count_clicks++; // Increase count
                l1.setText(String.valueOf(count_clicks)); // Set new count to label
            }
        });

        // Add button and label to layout
        f.add(b1);
        f.add(l1);

        // Set to center layout
        f.setLayout(new FlowLayout(FlowLayout.CENTER));

        // Set size of the window (width x height)
        f.setSize(300,300);

        // Show the window
        f.setVisible(true);
    }

    public static void main(String[] args) {
        // Create an anonymous object of Main in order to trigger the constructor defined above ^.
        new Main();
    }
}