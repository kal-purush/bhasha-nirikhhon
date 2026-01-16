package de.hebk.gui;

import javax.swing.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

public class EndGui {
    private JPanel panel1;
    private JButton backToMainMenuButton;
    private JLabel infoLabel;

    public EndGui(StartGui gui, String info) {
        infoLabel.setText(info);
        gui.setContentPane(panel1);
        gui.revalidate();
        gui.repaint();

        backToMainMenuButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                gui.setContentPane(gui.getPanel());
                gui.revalidate();
                gui.repaint();
            }
        });
    }

    private void createUIComponents() {
        infoLabel = new JLabel();
    }
}