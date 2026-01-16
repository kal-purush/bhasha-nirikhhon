package com.PredatorPrey;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionListener;

public class ControlsPanel extends JPanel {
    public ControlsPanel(ActionListener beginListener, ActionListener quitListener) {
        JButton beginButton = createTransparentButton("Begin");
        beginButton.addActionListener(beginListener);
        this.add(beginButton);

        JButton quitButton = createTransparentButton("Quit");
        quitButton.addActionListener(quitListener);
        this.add(quitButton);

        this.setBackground(Color.BLACK);
    }

    private JButton createTransparentButton(String text) {//makes a transparent button
        JButton button = new JButton(text);
        button.setFont(button.getFont().deriveFont(30f));
        button.setOpaque(false);
        button.setContentAreaFilled(false);
        button.setBorderPainted(false);
        button.setForeground(UIManager.getColor("Button.foreground"));
        return button;
    }

}