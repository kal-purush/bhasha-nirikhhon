package UserInterface;

import java.awt.BorderLayout;
import java.awt.Container;
import java.awt.Dimension;
import java.awt.Toolkit;
import javax.swing.BorderFactory;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.SwingUtilities;
import javax.swing.border.Border;

public class ProgressBar extends JPanel {

    JProgressBar jpbar;
    public static int MINIMUM_VALUE = 0;
    public static int MAXIMUM_VALUE = 100;
    public static int CURRENT_VALUE = 0;
    private static ProgressBar pbar;
    private static Border border;
    private static Dimension screenSize = Toolkit.getDefaultToolkit().getScreenSize();
    private static int width = screenSize.width;
    private static JFrame frame;

    public ProgressBar() {
        jpbar = new JProgressBar();
        jpbar.setMinimum(MINIMUM_VALUE);
        jpbar.setMaximum(MAXIMUM_VALUE);
        jpbar.setStringPainted(true);
        add(jpbar);
    }

    public void updateBar(int newValue) {
        jpbar.setValue(newValue);
    }

    public static void showProgress(String text,int percent) {
        int loaded_percent;
        int previous_value = CURRENT_VALUE;
        CURRENT_VALUE = CURRENT_VALUE + percent;
        if ( previous_value == 0 ) {
            //showing ProgressBar first time
            pbar = new ProgressBar();
            frame = new JFrame("Travels-Management-System");
            frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
            frame.setSize(width/3, 80);
            border = BorderFactory.createTitledBorder(text);
            pbar.setBorder(border);
            Container content = frame.getContentPane();
            content.add(pbar, BorderLayout.NORTH);
            frame.setVisible(true);
            frame.setLocationRelativeTo(null);
            frame.setContentPane(pbar);
            frame.setVisible(true);
            pbar.updateBar(CURRENT_VALUE);
        }else if (CURRENT_VALUE >= MAXIMUM_VALUE) {
            //Close ProgressBar when it is loaded completely
            pbar.updateBar(MAXIMUM_VALUE);
            frame.setVisible(false);
        }else {
            //Update the ProgressBar percent
            border = BorderFactory.createTitledBorder(text);
            pbar.setBorder(border);
            pbar.updateBar(CURRENT_VALUE);
            frame.setVisible(true);
        }
    }

    /* this method will show Progress bar anonymously */
    public static  void showProgress() {
        pbar = new ProgressBar();
        frame = new JFrame("Travels-Management-System");
        frame.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
        Container content = frame.getContentPane();
        frame.setSize(width / 3, 80);
        border = BorderFactory.createTitledBorder("Loading.....");
        pbar.setBorder(border);
        content.add(pbar, BorderLayout.NORTH);
        frame.setVisible(true);
        frame.setLocationRelativeTo(null);
        frame.setContentPane(pbar);
        frame.setVisible(true);
        for (int i = MINIMUM_VALUE; i <= MAXIMUM_VALUE; i++) {
            final int percent = i;
            try {
                SwingUtilities.invokeLater(new Runnable() {
                    public void run() {
                        pbar.updateBar(percent);
                    }
                });
                java.lang.Thread.sleep(10);
            } catch (InterruptedException e) {
                ;
            }
        }
    }
}