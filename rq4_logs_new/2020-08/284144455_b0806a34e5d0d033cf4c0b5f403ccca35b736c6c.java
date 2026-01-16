/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Second;

import java.awt.Color;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.WindowConstants;

/**
 *
 * @author Christopher
 */
public class VO {
      
      JFrame JF = new JFrame();
      JButton op1 = new JButton();
      JButton op2 = new JButton();
      
      public VO(){
            JF.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
            JF.setLayout(null);
            JF.setBackground(Color.BLACK);
            JF.setVisible(true);
            
            JF.add(op1);
            JF.add(op2);
            
            op1.setBounds(8, 18, 50, 27);
            op2.setBounds(62, 18, 50, 27);
            
            JF.setSize(106, 60);
      }
      
      public static void main(String[] args){
            new VO();
      }
}
