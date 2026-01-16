import java.awt.Font;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;

import javax.swing.JComboBox;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.SwingUtilities;
import javax.swing.JApplet;

public class DropDown extends JFrame implements ActionListener {
  JLabel fontLabel = new JLabel(
      "The quick brown fox jumps over the lazy dog's back.");

  private JComboBox fontComboBox;

  public DropDown() { 
    setTitle("ComboBoxTest");
    setSize(400, 300);
    addWindowListener(new WindowAdapter() {
      public void windowClosing(WindowEvent e) {
        System.exit(0);
      }
    });

    fontComboBox = new JComboBox();
    fontComboBox.setEditable(true);
    fontComboBox.addItem("Serif");
    fontComboBox.addItem("Arial");
    fontComboBox.addItem("Monospaced");
    fontComboBox.addItem("Zapfino");
    fontComboBox.addItem("Verdana");
    fontComboBox.addActionListener(this);

    JPanel p = new JPanel();
    p.add(fontComboBox);
    getContentPane().add(p, "North");
    getContentPane().add(fontLabel, "Center");
  }

  public void actionPerformed(ActionEvent evt) {
    JComboBox source = (JComboBox) evt.getSource();
    String item = (String) source.getSelectedItem();
    fontLabel.setFont(new Font(item, Font.PLAIN, 12));
  }
 

  public static void main(String[] args) {
    JFrame frame = new DropDown();
    frame.show();
  }
}

           

