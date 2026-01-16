/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Second;

import java.awt.Color;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JTree;

/**
 *
 * @author Christopher
 */
public class VP {
      
      JFrame JF = new JFrame();
      JPanel JP = new JPanel();
      JTree JTE = new JTree();
      
      public VP(){
            JF.setDefaultCloseOperation(2);
            JF.setLayout(null);
            JF.setSize(1070, 727);
            JF.setLocationRelativeTo(null);
            JF.setAlwaysOnTop(true);
            
            JF.add(JP);
            JP.setBackground(Color.BLACK);
            JP.setLayout(null);
            setPanelFitOnJFrame(JP, JF);
            
            JP.add(JTE);
            JTE.setBounds(4, 4, 180, JP.getHeight() - 8);
            JTE.setBackground(Color.LIGHT_GRAY.brighter());
            //+++++++++++++++++++++++
            JF.setVisible(true);
      }
      
      private void setPanelFitOnJFrame(JPanel jp, JFrame jf){
            jp.setBounds(2, 2, 
                    jf.getWidth() - 21, 
                    jf.getHeight() - 44);
            
            System.out.println("JP Bounds: " + jp.getBounds());
      }
      
      public static void main(String[] args){
            new VP();
      }
}