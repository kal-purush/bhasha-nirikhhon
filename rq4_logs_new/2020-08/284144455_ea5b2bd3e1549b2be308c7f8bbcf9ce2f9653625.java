/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Second;

import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

/**
 *
 * @author Christopher
 */
public class VO_AL implements ActionListener {

      private String QT;

      @Override
      public void actionPerformed(ActionEvent e) {
            if (QT.equals("Delete this table?")) {
                  if (e.getActionCommand().equals("Yes")) {
                        
                  }
                  VO.getJF().dispose();
            }
      }

      public VO_AL(String qt) {
            QT = qt;
      }

}