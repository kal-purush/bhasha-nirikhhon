package proyecto;
import java.awt.Color;
import java.awt.FlowLayout;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.UIManager;
import org.jdesktop.swingx.JXDatePicker;
import org.jdesktop.swingx.plaf.basic.CalendarHeaderHandler;
import org.jdesktop.swingx.plaf.basic.SpinningCalendarHeaderHandler;
import java.awt.Dimension;
import java.awt.Font;
import java.text.SimpleDateFormat;
import java.util.Locale;



public class RenderCalendar {
    

    public RenderCalendar(String LabelText, JPanel MainPanel){
        
        JPanel RowPanel = new JPanel(); // We create a Panel for the first row
        RowPanel.setLayout(new FlowLayout(FlowLayout.CENTER, 5, 10)); // Set it to FlowLayout so it can be aligned horizontally
        RowPanel.setBackground(Colores.almond);
    
        //We create the component label
        JLabel CompLabel = new JLabel(LabelText);
        CompLabel.setFont(new Font("Verdana", Font.PLAIN, 20));
        CompLabel.setBackground(Colores.almond);
        CompLabel.setOpaque(true);
        CompLabel.setHorizontalAlignment(JLabel.RIGHT);
        CompLabel.setPreferredSize(new Dimension(260,30));
    
        //We create the component DATE PICKER LETS GOOOOOOOOOOOOOO 
        UIManager.put(CalendarHeaderHandler.uiControllerID, SpinningCalendarHeaderHandler.class.getName()); 
        JXDatePicker picker = new JXDatePicker(new Locale("es")); //Initialize a JXDatePicker using the ISO language "es" for Spanish 
        picker.getMonthView().setZoomable(true); //This line of code, and the UIManager one enable an experimental feature to allow yearly navigation through the calendar
        picker.setFormats(new SimpleDateFormat("dd/MM/yyyy")); //Setting a date format
        picker.getEditor().setEnabled(false); //The JXDatePicker Component is essentially made of two embedded components: The calendar itself, and a FormattedTextField where the input is displayed, the .getEditor() method allows us to interact specifically with the TextField, on this case I disabled it, in order to force the user to enter a date only using the Date Picker
        picker.getEditor().setDisabledTextColor(Color.BLACK); 
        ShowHint.setHint("Seleccione su fecha de nacimiento",picker.getEditor());
        picker.setPreferredSize(new Dimension(250, 30));//After using the setEnabled(false) method, the FormattedTextField is visually disabled too, rendering the text in a transparent, blue-ish tone, this method overrides that color, making it look as if it wasn't disabled

 

    
        // Add the components to the panel
        RowPanel.add(CompLabel);
        RowPanel.add(picker);
    
        // Render it to the main frame (panel being rendered)
        MainPanel.add(RowPanel);
    }
        
}