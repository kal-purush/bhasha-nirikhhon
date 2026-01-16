package main;

import java.awt.EventQueue;
import javax.swing.JFrame;
import javax.swing.*;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.List;

public class AdminInterface extends JFrame {
    private JTextArea textAreaUtilisateurs;
    private JButton boutonRafraichir, boutonAjouter, boutonModifier, boutonSupprimer, boutonReinitialiser;
    private JTextField champEmail, champUsername, champFullname, champPassword;
    private JLabel labelMessage;

    public AdminInterface() {
        setTitle("Interface Administrateur");
        setSize(600, 400);
        setDefaultCloseOperation(JFrame.DISPOSE_ON_CLOSE);
        setLocationRelativeTo(null);

        JPanel panel = new JPanel();
        panel.setLayout(null);

        // Zone de texte pour afficher les utilisateurs
        textAreaUtilisateurs = new JTextArea();
        textAreaUtilisateurs.setBounds(10, 10, 400, 200);
        textAreaUtilisateurs.setEditable(false);
        JScrollPane scrollPane = new JScrollPane(textAreaUtilisateurs);
        scrollPane.setBounds(10, 10, 400, 200);
        panel.add(scrollPane);

        // Bouton pour rafraîchir la liste des utilisateurs
        boutonRafraichir = new JButton("Rafraîchir");
        boutonRafraichir.setBounds(420, 10, 150, 25);
        panel.add(boutonRafraichir);

        // Champ pour l'email
        JLabel labelEmail = new JLabel("Email:");
        labelEmail.setBounds(10, 220, 80, 25);
        panel.add(labelEmail);

        champEmail = new JTextField(20);
        champEmail.setBounds(100, 220, 250, 25);
        panel.add(champEmail);

        // Champ pour le nom d'utilisateur
        JLabel labelUsername = new JLabel("Username:");
        labelUsername.setBounds(10, 250, 80, 25);
        panel.add(labelUsername);

        champUsername = new JTextField(20);
        champUsername.setBounds(100, 250, 250, 25);
        panel.add(champUsername);

        // Champ pour le nom complet
        JLabel labelFullname = new JLabel("Fullname:");
        labelFullname.setBounds(10, 280, 80, 25);
        panel.add(labelFullname);

        champFullname = new JTextField(20);
        champFullname.setBounds(100, 280, 250, 25);
        panel.add(champFullname);

        // Champ pour le mot de passe
        JLabel labelPassword = new JLabel("Password:");
        labelPassword.setBounds(10, 310, 80, 25);
        panel.add(labelPassword);

        champPassword = new JTextField(20);
        champPassword.setBounds(100, 310, 250, 25);
        panel.add(champPassword);

        // Bouton pour ajouter un utilisateur
        boutonAjouter = new JButton("Ajouter");
        boutonAjouter.setBounds(420, 220, 150, 25);
        panel.add(boutonAjouter);

        // Bouton pour modifier un utilisateur
        boutonModifier = new JButton("Modifier");
        boutonModifier.setBounds(420, 250, 150, 25);
        panel.add(boutonModifier);

        // Bouton pour supprimer un utilisateur
        boutonSupprimer = new JButton("Supprimer");
        boutonSupprimer.setBounds(420, 280, 150, 25);
        panel.add(boutonSupprimer);

        // Bouton pour réinitialiser le mot de passe
        boutonReinitialiser = new JButton("Réinitialiser");
        boutonReinitialiser.setBounds(420, 310, 150, 25);
        panel.add(boutonReinitialiser);

        // Label pour les messages
        labelMessage = new JLabel("");
        labelMessage.setBounds(10, 340, 550, 25);
        panel.add(labelMessage);

        // Action pour le bouton Rafraîchir
        boutonRafraichir.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                rafraichirListeUtilisateurs();
            }
        });

        // Action pour le bouton Ajouter
        boutonAjouter.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                String username = champUsername.getText();
                String password = champPassword.getText();
                String fullname = champFullname.getText();
                String email = champEmail.getText();

                GestionLogin gestionLogin = new GestionLogin();
                try {
                    gestionLogin.ajouterUtilisateur(username, password, fullname, email);
                    labelMessage.setText("Utilisateur ajouté avec succès.");
                    rafraichirListeUtilisateurs();
                } catch (IllegalArgumentException ex) {
                    labelMessage.setText("Erreur : " + ex.getMessage());
                }
            }
        });

        // Action pour le bouton Modifier
        boutonModifier.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                String email = champEmail.getText();
                String username = champUsername.getText();
                String fullname = champFullname.getText();
                String nouveauEmail = champEmail.getText();

                GestionLogin gestionLogin = new GestionLogin();
                gestionLogin.mettreAJourUtilisateur(email, username, fullname, nouveauEmail);
                labelMessage.setText("Utilisateur modifié avec succès.");
                rafraichirListeUtilisateurs();
            }
        });

        // Action pour le bouton Supprimer
        boutonSupprimer.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                String email = champEmail.getText();

                GestionLogin gestionLogin = new GestionLogin();
                gestionLogin.supprimerUtilisateur(email);
                labelMessage.setText("Utilisateur supprimé avec succès.");
                rafraichirListeUtilisateurs();
            }
        });

        // Action pour le bouton Réinitialiser
        boutonReinitialiser.addActionListener(new ActionListener() {
            @Override
            public void actionPerformed(ActionEvent e) {
                String email = champEmail.getText();

                GestionLogin gestionLogin = new GestionLogin();
                String motDePasseTemporaire = gestionLogin.reinitialiserMotDePasse(email);
                labelMessage.setText("Mot de passe réinitialisé. Nouveau mot de passe : " + motDePasseTemporaire);
            }
        });

        add(panel);
        rafraichirListeUtilisateurs(); // Rafraîchir la liste au démarrage
    }

    /**
     * Rafraîchit la liste des utilisateurs dans la zone de texte.
     */
    private void rafraichirListeUtilisateurs() {
        GestionLogin gestionLogin = new GestionLogin();
        List<String[]> utilisateurs = gestionLogin.listerUtilisateurs();
        StringBuilder listeUtilisateurs = new StringBuilder();

        for (String[] utilisateur : utilisateurs) {
            listeUtilisateurs.append("Username: ").append(utilisateur[0])
                    .append(", Fullname: ").append(utilisateur[1])
                    .append(", Email: ").append(utilisateur[2])
                    .append("\n");
        }

        textAreaUtilisateurs.setText(listeUtilisateurs.toString());
    }
}