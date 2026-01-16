package proyecto.Renderers;
import java.awt.Dimension;
import java.awt.Font;
import java.awt.FlowLayout;
import proyecto.Colores;
import javax.swing.JPanel;
import javax.swing.JRadioButton;
import javax.swing.ButtonGroup;
import javax.swing.JComboBox;
import javax.swing.JLabel;

public class RenderBloodType {


    public RenderBloodType(JPanel MainPanel){
        JPanel RowPanel = new JPanel(); // We create a Panel for the first row
        RowPanel.setLayout(new FlowLayout(FlowLayout.CENTER, 5, 10)); // Set it to FlowLayout so it can be aligned horizontally
        RowPanel.setBackground(Colores.almond);
    
        //We create the component label
        JLabel CompLabel = new JLabel("Tipo de Sangre:");
        CompLabel.setFont(new Font("Verdana", Font.PLAIN, 20));
        CompLabel.setBackground(Colores.almond);
        CompLabel.setOpaque(true);
        CompLabel.setHorizontalAlignment(JLabel.RIGHT);
        CompLabel.setPreferredSize(new Dimension(260,30));
        
        String bloodTypes[] = {"O+", "A+", "B+", "AB+", "O-", "A-", "B-", "AB-"};
        JComboBox bloodMenu = new JComboBox(bloodTypes);
        bloodMenu.setPreferredSize(new Dimension(250,30));
        bloodMenu.setSelectedItem(null);
        
        // Add the components to the panel
        RowPanel.add(CompLabel);
        RowPanel.add(bloodMenu);

   
        // Render it to the main frame (panel being rendered)
        MainPanel.add(RowPanel);
    }
}