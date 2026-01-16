package vAlpha2;

import vAlpha1.*;
import javax.swing.*;
import java.awt.*;
//import java.awt.event.ActionEvent;
//import java.awt.event.ActionListener;

public class PanDisp extends JPanel // panel definition
{
    //private JLabel lblName;
    //private String sLabel;

    public PanDisp() {
        setBackground(Color.black);
        //lblName = new JLabel(" Barney");
        JButton btn1 = new JButton("Start");
        add(btn1);
        //add(lblName); //add it to the Frame
    }

    /*void UpdateLabel(String _sNew) {
        sLabel = _sNew;
        lblName.setText(sLabel);
    }    */
}