/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Listeners;

import MC.DT;
import MC.MM;
import Others.CC;
import Second.VC_R;
import java.awt.Color;
import java.awt.event.KeyEvent;
import java.awt.event.KeyListener;
import java.util.regex.Pattern;

/**
 *
 * @author Christopher
 */
public class TF_KL_VC implements KeyListener {

      //int count = 0;
      
      @Override
      public void keyTyped(KeyEvent e) {

      }

      @Override
      public void keyPressed(KeyEvent e) {

      }

      @Override
      public void keyReleased(KeyEvent e) {
            boolean bool = false;
            System.out.println(e.getComponent().getName());

            for (int a = 0; a < VC_R.getJTFS().length; a++) {
                  String text = VC_R.getJTFS()[a].getText().toUpperCase();

                  for (int b = 0; b < DT.getBandW().size(); b++) {
                        if (text.contains(DT.getBandW().get(b))) {
                              //System.out.println(CC.RED + "\t"+ (++count) + 
                                //            ": NO" + CC.RESET);
                              VC_R.getJTFS()[a].setForeground(Color.RED);
                              bool = true;
                        }  else {
                              if (bool != true) {
                                    //System.out.println(CC.GREEN + "\t"+ (++count) + 
                                     //       ": YES" + CC.RESET);
                                    VC_R.getJTFS()[a].setForeground(Color.WHITE);

                              }
                        }
                  }
            }
      }

}