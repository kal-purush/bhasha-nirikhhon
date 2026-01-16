package Views;

import Models.Bibliothecaire;
import Models.Personne;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.HashMap;

public class LoginPage {

    private boolean authenticated = false;
    private Bibliothecaire currentBibliothecaire = null;

    private HashMap<String, Bibliothecaire> bibliothecaires;

    public LoginPage(HashMap<String, Bibliothecaire> bibliothecaires) {
        this.bibliothecaires = bibliothecaires;
    }

    public boolean showLoginDialog(JFrame parentFrame) {
        JDialog loginDialog = new JDialog(parentFrame, "Authentification", true);
        loginDialog.setSize(300, 150);
        loginDialog.setLayout(new GridLayout(3, 2));

        JLabel userLabel = new JLabel("Nom Utilisateur:");
        JTextField userField = new JTextField();
        JLabel passLabel = new JLabel("Mot de Passe:");
        JPasswordField passField = new JPasswordField();
        JButton loginButton = new JButton("Valider");

        // Ajouter les composants à la fenêtre
        loginDialog.add(userLabel);
        loginDialog.add(userField);
        loginDialog.add(passLabel);
        loginDialog.add(passField);
        loginDialog.add(new JLabel(""));
        loginDialog.add(loginButton);

        // Action du bouton de login
        loginButton.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                String username = userField.getText();
                String password = new String(passField.getPassword());

                // Vérifier si le bibliothécaire existe et si le mot de passe est correct
                if (bibliothecaires.containsKey(username)) {
                    Bibliothecaire bibliothecaire = bibliothecaires.get(username);
                    if (bibliothecaire.getMdp().equals(password)) {
                        authenticated = true;
                        currentBibliothecaire = bibliothecaire;
                        loginDialog.dispose();  // Fermer la boîte de dialogue si la connexion est réussie
                    } else {
                        JOptionPane.showMessageDialog(loginDialog, "Invalid password", "Error", JOptionPane.ERROR_MESSAGE);
                    }
                } else {
                    JOptionPane.showMessageDialog(loginDialog, "Invalid username", "Error", JOptionPane.ERROR_MESSAGE);
                }
            }
        });

        loginDialog.setLocationRelativeTo(parentFrame); // Centrer par rapport à la fenêtre principale
        loginDialog.setVisible(true);  // Afficher la boîte de dialogue

        return authenticated;
    }

    public Bibliothecaire getCurrentBibliothecaire() {
        return currentBibliothecaire;
    }
}