package org.argouml.cognitive;

import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import javax.swing.JComponent;
import javax.swing.JPanel;

public class LayoutHelper {

    public static void addComponent(JPanel panel, JComponent component, GridBagLayout gb, GridBagConstraints c,
                                    int gridx, int gridy, int gridwidth, int gridheight, double weightx, int fill) {
        c.gridx = gridx;
        c.gridy = gridy;
        c.gridwidth = gridwidth;
        c.gridheight = gridheight;
        c.weightx = weightx;
        c.fill = fill;
        gb.setConstraints(component, c);
        panel.add(component);
    }
}