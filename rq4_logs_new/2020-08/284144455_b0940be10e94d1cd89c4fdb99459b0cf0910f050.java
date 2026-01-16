package smallComponenets;

import MC.DT;
import java.awt.GradientPaint;
import java.awt.Graphics;
import java.awt.Graphics2D;
import javax.swing.JPanel;

/**
 *
 * @author Christopher
 */
public class PanelGradient extends JPanel {

      @Override
      public void paintComponent(Graphics g) {
            Graphics2D g2 = (Graphics2D) g;
            g2.setPaint(new GradientPaint(
                    0, 0, DT.GP_darkRed[0],
                    getWidth(), getHeight(),
                    DT.GP_darkRed[1]));
            g2.fillRect(0, 0, getWidth(), getHeight());
            
            super.paintComponents(g);
      }
}