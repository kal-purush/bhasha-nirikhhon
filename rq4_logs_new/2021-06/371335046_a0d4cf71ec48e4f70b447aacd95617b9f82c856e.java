import GLOOP.GLObjekt;

import javax.swing.*;
import java.awt.*;

public class ListMenu {
    public ListMenu() {
        JFrame frame = new JFrame();
        frame.setTitle("Objects");
        frame.setLocationRelativeTo(null);

        JPanel panel = new JPanel();
        int size = Main.objectList.size();
        if (size < 1) size = 1;
        panel.setLayout(new GridLayout(size, 0));

        JLabel title = new JLabel("All Objects:");
        panel.add(title);
        for (GLObjekt obj : Main.objectList) {
            JLabel label = new JLabel(obj.toString());
            panel.add(label);
        }

        frame.add(panel);
        frame.pack();
        frame.setVisible(true);
    }
}