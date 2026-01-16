package avegetablegarden.vuecontroleur.vues.windows.editpotager;

import avegetablegarden.modele.potagers.Potager;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

public class EditName extends JFrame {

    private JTextField potagerNameTextField;
    private Potager potager;

    public EditName(Potager potager) {
        super("Éditer le nom du potager");
        this.potager = potager;
        initWindow();
        potagerNameTextField.setText(potager.getName());

        pack();
        setVisible(true);
    }

    private void initWindow() {
        JLabel filenameLabel = new JLabel("Nom du potager :");
        potagerNameTextField = new JTextField();
        int padding = 10;

        JButton saveButton = addSaveButton();
        saveButton.setPreferredSize(new Dimension(0, saveButton.getPreferredSize().height));

        JPanel panel = new JPanel(new GridLayout(3, 1, padding, padding));
        panel.setBorder(BorderFactory.createEmptyBorder(padding, padding, padding, padding));
        panel.add(filenameLabel);
        panel.add(potagerNameTextField);
        panel.add(saveButton);

        getContentPane().add(panel, BorderLayout.CENTER);
        this.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
        this.setPreferredSize(new Dimension(300, 150));
        this.setLocationRelativeTo(null);
    }

    private JButton addSaveButton() {
        JButton saveButton = new JButton("Sauvegarder");
        saveButton.addActionListener(new EditName.SaveButtonListener());
        return saveButton;
    }

    private class SaveButtonListener implements ActionListener {

        @Override
        public void actionPerformed(ActionEvent e) {
            String potagerName = potagerNameTextField.getText();
            if (potagerName.isEmpty()) {
                JOptionPane.showMessageDialog(EditName.this, "Veuillez saisir un nom de potager valide", "Erreur", JOptionPane.ERROR_MESSAGE);
                return;
            }
            potager.setName(potagerName);
            dispose();
        }
    }
}