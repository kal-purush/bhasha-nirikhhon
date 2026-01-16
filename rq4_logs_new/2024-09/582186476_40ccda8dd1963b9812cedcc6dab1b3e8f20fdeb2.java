package KeyboardHelp.Components;

import javax.swing.*;
import java.awt.*;

public class JLabelNormal extends JLabel {
    private int x,y;
    private int width = 200;
    private int height = 50 ;
    public JLabelNormal(String title, int x, int y){
        super(title);
        this.setBounds(x,y, width,height);
        this.setForeground(Color.YELLOW);
        this.setFont(new Font("Calibri", Font.BOLD, 18));
        this.setHorizontalAlignment(JLabel.LEFT);
        this.setVerticalAlignment(JLabel.CENTER);
    }
}