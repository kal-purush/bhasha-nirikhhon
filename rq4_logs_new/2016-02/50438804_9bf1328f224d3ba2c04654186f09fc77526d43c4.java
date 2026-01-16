import java.awt.*;
import java.awt.event.*;
import javax.swing.*;

public class FontDropDowns {

   private JFrame mainFrame;
   private JLabel headerLabel;
   private JLabel statusLabel;
   private JPanel controlPanel;

   public FontDropDowns(){
      prepareGUI();
   }

   public static void main(String[] args){
      FontDropDowns FontDropDowns = new FontDropDowns();  
      FontDropDowns.showEventDemo();       
   }
      
   private void prepareGUI(){
      mainFrame = new JFrame("Font Drop Down Menus");
      mainFrame.setSize(600,400);
      mainFrame.setLayout(new GridLayout(2, 1));

      headerLabel = new JLabel("",JLabel.CENTER);
      statusLabel = new JLabel("",JLabel.CENTER);        

      statusLabel.setSize(350,100);
      mainFrame.addWindowListener(new WindowAdapter() {
         public void windowClosing(WindowEvent windowEvent){
	        System.exit(0);
         }        
      });    
      controlPanel = new JPanel();
      controlPanel.setLayout(new FlowLayout());

      mainFrame.add(headerLabel);
      mainFrame.add(controlPanel);
      mainFrame.add(statusLabel);
      mainFrame.setVisible(true);  
   }

   private void showEventDemo(){
      headerLabel.setText("JComboBox"); 
      GraphicsEnvironment ge = GraphicsEnvironment.getLocalGraphicsEnvironment();
      String[] names = ge.getAvailableFontFamilyNames();
      
      JComboBox<String> fontFamily = new JComboBox<String>();
      JComboBox<String> fontStyle  = new JComboBox<String>();

      for ( int i=0; i<names.length; i++ ){
    	  
    	  fontFamily.addItem(names[i]);
    	 
      } 

      controlPanel.add(fontFamily);
      
      mainFrame.setVisible(true);  
   }
   }