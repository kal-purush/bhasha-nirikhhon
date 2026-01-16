package vueControleur.vues.windows;

import modele.potagers.Potager;
import modele.potagers.SimulateurPotager;

import javax.swing.*;
import java.awt.*;
import java.util.List;

public class EditPotagersWindow extends JFrame {
    SimulateurPotager simulateurPotager;

    public EditPotagersWindow(SimulateurPotager simulateurPotager) {
        super("Éditer les potagers");
        this.simulateurPotager = simulateurPotager;
        initWindow();
    }

    private void initWindow() {
        setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
        setSize(400, 600);
        setLocationRelativeTo(null);

        // each line is a potager
        // different columns : edit name, edit color, delete
        JPanel panel = new JPanel();
        panel.setLayout(new BoxLayout(panel, BoxLayout.Y_AXIS));

        List<Potager> potagers = simulateurPotager.getListePotagers();

        for (int i = 0; i < potagers.size(); i++) {
            JPanel potagersPanel = new JPanel();
            potagersPanel.setLayout(new GridLayout(1, simulateurPotager.getNbPotagers()));
            JPanel potagerPanel = new JPanel();
            potagerPanel.setLayout(new GridLayout(2, 3));

            potagerPanel.add(new JLabel(potagers.get(i).getName()));
            potagerPanel.add(new JLabel(""));
            potagerPanel.add(new JLabel(""));
            potagerPanel.add(new JButton("Change name"));
            potagerPanel.add(new JButton("Change color"));
            potagerPanel.add(new JButton("Delete"));
            potagersPanel.add(potagerPanel);
            panel.add(potagersPanel);
        }

        add(panel);
    }
}