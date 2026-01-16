/*
    Drew Heaton
    Software Engineering 1
    April 6, 2015
    Class that defines the styles and attributes for the buttons on the main menu
*/

package ball_app;

import java.awt.Color;
import java.awt.Font;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import javax.swing.JButton;

public class MenuButton extends JButton {
    
    public MenuButton(String text) {
        
        // set style properties
        setOpaque(false);
        setContentAreaFilled(false);
        setBorderPainted(false);
        setText(text);
        setForeground(Color.WHITE);
        setFont(new Font("Arial", Font.PLAIN, 20));
        
        // set listeners for hover behavior
        addMouseListener(new MouseAdapter() {
            @Override
            public void mouseEntered(MouseEvent e) {
                setFont(new Font("Arial", Font.BOLD, 20));
            }
            
            @Override
            public void mouseExited(MouseEvent e) {
                setFont(new Font("Arial", Font.PLAIN, 20));
            }
        });
    }
}