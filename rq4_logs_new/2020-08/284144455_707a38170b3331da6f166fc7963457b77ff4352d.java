/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Listeners_C;

import MC.DT;
import Second.VC_R;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import javax.swing.JComponent;

/**
 *
 * @author Christopher
 */
public class BTN_CK_AL implements ActionListener{

      @Override
      public void actionPerformed(ActionEvent e) {
            //System.out.println(((JComponent)e.getSource()).getName());
            for(int a = 0; a < DT.maxColumns - 1; a++){
                  if(e.getSource() == VC_R.getBTNS_Clock()[a]){
                        if(VC_R.getBTNS_Clock()[a].isSelected()){
                            for(int b = 0; b < DT.maxColumns - 1; b++){
                                  if(b != a){
                                        VC_R.getBTNS_Clock()[b].setSelected(false);
                                  }
                            }
                        }
                  }else if(e.getSource() == VC_R.getBTNS_Tabl()[a]){
                         if(VC_R.getBTNS_Tabl()[a].isSelected()){
                            for(int b = 0; b < DT.maxColumns - 1; b++){
                                  if(b != a){
                                        VC_R.getBTNS_Tabl()[b].setSelected(false);
                                  }
                            }
                        }
                  }
            }
      }
      
}