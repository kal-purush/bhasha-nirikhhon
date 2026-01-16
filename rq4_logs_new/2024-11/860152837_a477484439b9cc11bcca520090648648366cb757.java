package org.argouml.ui;

import java.awt.Component;
import javax.swing.BoxLayout;
import javax.swing.JLabel;
import javax.swing.JPanel;

import org.argouml.i18n.Translator;
import org.argouml.swingext.JLinkButton;

public class WarningPanel extends JPanel {

    public WarningPanel() {
        setLayout(new BoxLayout(this, BoxLayout.PAGE_AXIS));

        JLabel warningLabel = new JLabel(Translator.localize("label.warning"));
        warningLabel.setAlignmentX(Component.RIGHT_ALIGNMENT);
        add(warningLabel);

        JLinkButton projectSettings = new JLinkButton();
        projectSettings.setAction(new ActionProjectSettings());
        projectSettings.setText(Translator.localize("button.project-settings"));
        projectSettings.setIcon(null);
        projectSettings.setAlignmentX(Component.RIGHT_ALIGNMENT);
        add(projectSettings);
    }
}