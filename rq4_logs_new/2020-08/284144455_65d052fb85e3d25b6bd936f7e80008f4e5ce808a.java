package Listeners;

import MC.DT;
import Others.CC;
import Second.VC_R;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

/**
 *
 * @author Christopher
 */
public class BTN_PM_VC implements ActionListener {

      @Override
      public void actionPerformed(ActionEvent e) {
            
            
            for (int a = 0; a < DT.maxColumns - 2; a++) {
                  System.out.println(CC.YELLOW + 
                          "a = " + a + " // JC = " + VC_R.getJBTNS_P()[a + 1].getName());
                  
                  if(e.getSource() == VC_R.getJBTNS_M()[a + 2]){
                        VC_R.getJLBS()[a + 2].setVisible(false);
                        VC_R.getJTFS()[a + 2].setVisible(false);
                        VC_R.getJBTNS_P()[a + 2].setVisible(false);
                        VC_R.getJBTNS_M()[a + 2].setVisible(false);
                        
                        VC_R.getJBTNS_P()[a + 1].setVisible(true);
                        VC_R.getJBTNS_M()[a + 1].setVisible(true);
                  }
                  
                  //++++++++++++++++++++++
                  if (e.getSource() == VC_R.getJBTNS_P()[a + 1]) {      
                        VC_R.getJLBS()[a + 2].setVisible(true);
                        VC_R.getJTFS()[a + 2].setVisible(true);
                        VC_R.getJBTNS_P()[a + 2].setVisible(true);
                        VC_R.getJBTNS_M()[a + 2].setVisible(true);

                        VC_R.getJBTNS_P()[a + 1].setVisible(false);
                        VC_R.getJBTNS_M()[a + 1].setVisible(false);
                  }
            }
      }

}