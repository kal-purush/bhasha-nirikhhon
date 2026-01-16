package com.example.ais_v5.client;

import javax.swing.*;
import java.awt.*;

public class AdminPanel extends JFrame {
    public AdminPanel() {
        setTitle("Admin Dashboard - " + LoginPanel.AuthContext.getUsername());
        setSize(800, 600);
        setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        setLocationRelativeTo(null);

        JTabbedPane tabbedPane = new JTabbedPane();

        // Users management tab
        JPanel usersPanel = new JPanel();
        usersPanel.add(new JLabel("Users Management"));
        tabbedPane.addTab("Users", usersPanel);

        // Groups management tab
        JPanel groupsPanel = new JPanel();
        groupsPanel.add(new JLabel("Groups Management"));
        tabbedPane.addTab("Groups", groupsPanel);

        // Subjects management tab
        JPanel subjectsPanel = new JPanel();
        subjectsPanel.add(new JLabel("Subjects Management"));
        tabbedPane.addTab("Subjects", subjectsPanel);

        add(tabbedPane);

        // Add logout button
        JButton logoutButton = new JButton("Logout");
        logoutButton.addActionListener(e -> {
            this.dispose();
            new LoginPanel().setVisible(true);
        });
        add(logoutButton, BorderLayout.SOUTH);
    }
}