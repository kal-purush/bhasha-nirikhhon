package vueControleur.vues.components;

import vueControleur.icon.IconRepository;
import javax.swing.*;
import java.awt.*;
public class ButtonsPanel {
    private static final Font panelFont = new Font("Arial", Font.BOLD, 14);
    private static final Font subPanelFont = new Font("Arial", Font.BOLD, 12);
    private JPanel buttonsPanel;


    private static JLabel initTitle() {
        JLabel panelTitle = new JLabel("Actions");
        panelTitle.setAlignmentX(Component.CENTER_ALIGNMENT);
        panelTitle.setFont(panelFont);
        panelTitle.setMinimumSize(new Dimension(100, 20));
        return panelTitle;
    }

    private static JPanel initPanel() {
        JPanel buttons = new JPanel();
        buttons.setBackground(Color.WHITE);
        buttons.setPreferredSize(new Dimension(130, 200));
        int padding = 10;
        buttons.setBorder(BorderFactory.createEmptyBorder(padding, padding, padding, padding));
        buttons.setLayout(new BoxLayout(buttons, BoxLayout.Y_AXIS));
        return buttons;
    }

    private static void addPaddingBetweenComponents(JPanel panel) {
        panel.add(Box.createRigidArea(new Dimension(0, 10)));
    }

    private static JButton getPlantingButton() {
        JButton plantingButton = new JButton("Planter");
        plantingButton.setAlignmentX(Component.CENTER_ALIGNMENT);
        plantingButton.setFont(subPanelFont);
        plantingButton.setEnabled(true);
        plantingButton.addActionListener(e -> {
            // Ouvrir la fenêtre de plantation ici
            ImageIcon[] options = IconRepository.getInstance().getIcones();
            JOptionPane.showOptionDialog(null, "Fenêtre de plantation",
                    "Planter", JOptionPane.DEFAULT_OPTION, JOptionPane.PLAIN_MESSAGE, null, options, null);
        });
        return plantingButton;
    }

    private static JButton getWateringButton() {
        JButton wateringButton = new JButton("Arroser");
        wateringButton.setAlignmentX(Component.CENTER_ALIGNMENT);
        wateringButton.setFont(subPanelFont);
        wateringButton.setEnabled(true);
        return wateringButton;
    }

    private static JButton getHarvestingButton() {
        JButton harvestingButton = new JButton("Récolter");
        harvestingButton.setAlignmentX(Component.CENTER_ALIGNMENT);
        harvestingButton.setFont(subPanelFont);
        harvestingButton.setEnabled(true);
        return harvestingButton;
    }

    private void addPaddingBetweenSubComponents(JPanel panel) {
        panel.add(Box.createRigidArea(new Dimension(0, 5)));
    }

    private void initButtonsPanel() {
        JButton plantingButton;
        JButton wateringButton;
        JButton harvestingButton;
        buttonsPanel = new JPanel();
        buttonsPanel.setLayout(new BoxLayout(buttonsPanel, BoxLayout.Y_AXIS));
        buttonsPanel.setAlignmentX(Component.CENTER_ALIGNMENT);
        buttonsPanel.setBackground(Color.WHITE);

        addPaddingBetweenSubComponents(buttonsPanel);

        plantingButton = getPlantingButton();
        buttonsPanel.add(plantingButton);

        addPaddingBetweenSubComponents(buttonsPanel);

        wateringButton = getWateringButton();
        buttonsPanel.add(wateringButton);

        addPaddingBetweenSubComponents(buttonsPanel);

        harvestingButton = getHarvestingButton();
        buttonsPanel.add(harvestingButton);
    }

    public JPanel getButtonsPanel() {
        JPanel panel = initPanel();

        JLabel panelTitle = initTitle();
        panel.add(panelTitle);

        addPaddingBetweenComponents(panel);

        initButtonsPanel();
        panel.add(buttonsPanel);

        return panel;
    }
}