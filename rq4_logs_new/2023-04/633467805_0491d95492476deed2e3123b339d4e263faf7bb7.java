package a_vegetable_garden.vue_controleur.vues.components;

import javax.swing.*;
import java.awt.*;

public abstract class Panel {
    protected static final Font panelFont = new Font("Arial", Font.BOLD, 14);
    protected static final Font subPanelFont = new Font("Arial", Font.BOLD, 12);
    protected JPanel mainPanel;

    protected JLabel initTitle(String titre) {
        JLabel panelTitle = new JLabel(titre);
        panelTitle.setAlignmentX(Component.CENTER_ALIGNMENT);
        panelTitle.setFont(panelFont);
        panelTitle.setMinimumSize(new Dimension(100, 20));
        return panelTitle;
    }

    protected static JPanel initPanel() {
        JPanel infos = new JPanel();
        infos.setBackground(Color.WHITE);
        infos.setPreferredSize(new Dimension(130, 200));
        int padding = 10;
        infos.setBorder(BorderFactory.createEmptyBorder(padding, padding, padding, padding));
        infos.setLayout(new BoxLayout(infos, BoxLayout.Y_AXIS));
        return infos;
    }

    protected static void addPaddingBetweenComponents(JPanel panel) {
        panel.add(Box.createRigidArea(new Dimension(0, 10)));
    }

    protected void addPaddingBetweenSubComponents(JPanel panel) {
        panel.add(Box.createRigidArea(new Dimension(0, 5)));
    }

    public abstract JPanel getPanel();

    protected abstract void initMainPanel();
}