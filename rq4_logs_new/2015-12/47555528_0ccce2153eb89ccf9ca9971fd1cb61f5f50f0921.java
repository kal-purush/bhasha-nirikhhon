/*
 * To change this template, choose Tools | Templates
 * and open the template in the editor.
 */
package vAlpha1;

import javax.swing.*;
import java.awt.*;
/**
 *
 * @author gajdp9323
 */
public class PanGame {

public class PanGame extends JPanel // panel definition
{
    private JLabel lblName;
    private String sLabel;

    public PanGame() {
        //lblName = new JLabel(" Barney");
        //add(lblName); //add it to the Frame
    }

    void UpdateLabel(String _sNew) {
        sLabel = _sNew;
        lblName.setText(sLabel);
    }    
}
}