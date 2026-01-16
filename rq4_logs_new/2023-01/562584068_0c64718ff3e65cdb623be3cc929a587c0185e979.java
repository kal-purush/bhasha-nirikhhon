package de.hebk.gui.trueOrNot;

import de.hebk.gui.StartGui;

import javax.swing.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

public class Lose {
    private JPanel panel1;
    private JButton beendenButton;
    private JButton zurueckZumHauptmenueButton1;
    private JLabel losetext;

    public Lose(StartGui gui, int money){
        gui.setContentPane(panel1);
        gui.revalidate();
        gui.repaint();

        beendenButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                System.exit(0);
            }
        });

        zurueckZumHauptmenueButton1.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                StartGui haupt = new StartGui();
            }
        });
    }
    private void createUIComponents() {
        losetext = new JLabel();
    }
}