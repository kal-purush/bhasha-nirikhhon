package a_vegetable_garden.vue_controleur.vues.components;

import a_vegetable_garden.modele.legumes.Varietes;
import a_vegetable_garden.modele.potagers.Actions;
import a_vegetable_garden.vue_controleur.VueManager;
import a_vegetable_garden.vue_controleur.icon.IconNames;
import a_vegetable_garden.vue_controleur.icon.IconRepository;

import javax.swing.*;
import java.awt.*;
import java.awt.event.ActionListener;

public class ActionsButtonsPanel extends Panel {
    private static Varietes currentVariete = null;
    private static Actions currentAction = null;

    public static JButton getButton(Actions action) {
        JButton button;
        ImageIcon icon;
        ActionListener actionListener;
        switch (action) {
            case PLANTER -> {
                icon = IconRepository.getInstance().getIcone(IconNames.PLANTER);
                button = new JButton("Planter", icon);
                actionListener = e -> planterOnClick();
            }
            case ARROSER -> {
                icon = IconRepository.getInstance().getIcone(IconNames.ARROSER);
                button = new JButton("Arroser", icon);
                actionListener = e -> setAction(Actions.ARROSER);
            }
            case RECOLTER -> {
                icon = IconRepository.getInstance().getIcone(IconNames.RECOLTER);
                button = new JButton("Récolter", icon);
                actionListener = e -> setAction(Actions.RECOLTER);
            }
            default -> throw new IllegalStateException("Unexpected value: " + action);
        }

        button.setAlignmentX(Component.CENTER_ALIGNMENT);
        button.setFont(subPanelFont);
        button.setEnabled(true);
        VueManager.getInstance().changeCursor(Actions.RECOLTER);
        button.addActionListener(actionListener);
        return button;
    }

    private static void planterOnClick() {
        setAction(Actions.PLANTER);
        // Récupérer les boutons des variétés
        JButton[] buttons = ButtonsSelectVariete.getInstance().getButtons();
        // Ouvrir la fenêtre de plantation ici
        JDialog dialog = new JDialog();
        dialog.setTitle("Fenêtre de plantation");
        dialog.setLayout(new GridLayout(0, 1)); // 1 colonne, nombre de lignes automatique
        for (JButton button : buttons) {
            dialog.add(button);
        }
        // Ajouter un ActionListener à chaque bouton pour fermer le JDialog et récupérer l'index du bouton cliqué
        for (int i = 0; i < buttons.length; i++) {
            int finalI = i;
            buttons[i].addActionListener(e1 -> {
                dialog.dispose(); // Fermer la boîte de dialogue
                // Ordre des boutons = ordre des variétés
                currentVariete = Varietes.values()[finalI];
            });
        }
        // Afficher la boîte de dialogue modale
        dialog.setModal(true);
        dialog.pack();
        dialog.setLocationRelativeTo(null); // Centrer la boîte de dialogue
        dialog.setVisible(true);
    }

    protected void initMainPanel() {
        mainPanel = new JPanel();
        mainPanel.setLayout(new GridBagLayout());
        GridBagConstraints c = new GridBagConstraints();
        c.fill = GridBagConstraints.HORIZONTAL;
        c.gridx = 0;
        mainPanel.setAlignmentX(Component.CENTER_ALIGNMENT);
        mainPanel.setBackground(Color.WHITE);

        addPaddingBetweenSubComponents(mainPanel);

        addActionButtonToPanel(c, Actions.PLANTER, 0);

        addPaddingBetweenSubComponents(mainPanel);

        addActionButtonToPanel(c, Actions.ARROSER, 1);

        addPaddingBetweenSubComponents(mainPanel);

        addActionButtonToPanel(c, Actions.RECOLTER, 2);
    }

    private void addActionButtonToPanel(GridBagConstraints c, Actions action, int gridyIndex) {
        JButton button = getButton(action);
        c.gridy = gridyIndex;
        mainPanel.add(button, c);
    }

    public JPanel getPanel() {
        JPanel panel = initPanel(130, 130);

        JLabel panelTitle = initTitle("Actions");
        panel.add(panelTitle);

        addPaddingBetweenComponents(panel);

        initMainPanel();
        panel.add(mainPanel);

        return panel;
    }

    public Actions getAction() {
        return currentAction;
    }

    private static void setAction(Actions action) {
        currentAction = action;
        VueManager.getInstance().changeCursor(action);
    }

    public Varietes getCurrentVariete() {
        return currentVariete;
    }
}