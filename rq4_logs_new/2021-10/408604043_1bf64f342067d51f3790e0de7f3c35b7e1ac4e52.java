/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package plant_game;

import java.awt.BorderLayout;
import java.awt.Font;
import java.awt.event.ActionListener;
import javax.swing.JButton;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.SwingConstants;

/**
 *
 * @author breco
 */
public class ViewLoadGame extends JPanel {

    /**
     * @return the loadGameBack
     */
    public JButton getLoadGameBack() {
        return loadGameBack;
    }

    /**
     * @return the loadButtons
     */
    public JButton[] getLoadButtons() {
        return loadButtons;
    }

    private JLabel selectLoad = new JLabel("Select game to load", SwingConstants.CENTER);

    private JPanel loadGamePanel = new JPanel(new BorderLayout());
    private JPanel loadGamePanelArange = new JPanel();
    private JButton[] loadButtons = new JButton[5];

    private JButton loadGameBack = new JButton("Back");

    private Font fancy = new Font("verdana", Font.BOLD | Font.ITALIC, 28);

    public ViewLoadGame() {

        this.setLayout(new BorderLayout());
        this.selectLoad.setFont(fancy);

        //Set up 5 load game buttons 
        for (int i = 0; i < 5; i++) {
            this.loadButtons[i] = new JButton("" + i);
        }
        //Adds them to a panel arranging them ina  line.
        for (JButton load : getLoadButtons()) {
            this.loadGamePanelArange.add(load);
        }

        this.loadGamePanelArange.add(this.loadGameBack);
        //Adds the panel to the centre display of the load game panely.
        add(loadGamePanelArange, BorderLayout.SOUTH);
        add(this.selectLoad, BorderLayout.CENTER);

    }

    /**
     * @param loadGameBack the loadGameBack to set
     */
    public void setLoadGameBack(JButton loadGameBack) {
        this.loadGameBack = loadGameBack;
    }

    public void addActionListener(ActionListener actionListener) {
        getLoadGameBack().addActionListener(actionListener);
        //Action listeners for laod game buttons.
        for (JButton loadButton : getLoadButtons()) {
            loadButton.addActionListener(actionListener);
        }
    }
}