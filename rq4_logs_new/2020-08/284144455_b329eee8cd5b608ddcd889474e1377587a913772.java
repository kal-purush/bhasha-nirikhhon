package Listeners_C;

import MC.DT;
import MC.MakeCon;
import Others.CC;
import Second.VC_R;
import java.awt.Color;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;
import java.util.ArrayList;

/**
 *
 * @author Christopher
 */
public class BTN_Create_AL implements ActionListener {

      String CName = getClass().getName();

      MakeCon mc = new MakeCon(CName, DT.CCount++);

      int countO = -1;
      final String NS = "NONE";

      @Override
      public void actionPerformed(ActionEvent e) {
            System.out.println(CC.GREEN + "\nCREATE TABLE" + CC.RESET);
            int countV = 1;

            //HOW MANY COLS ARE VISIBLE
            for (int a = 0; a < DT.maxColumns - 1; a++) {
                  if (VC_R.getJTFS()[a + 1].isVisible()) {
                        countV++;
                  }
            }
            System.out.println("'\tColumns Visible: " + countV);

            TFSControl(countV);
            System.out.println("'\tColumns OK: " + countO);
            System.out.print("'\tMATCH ");
            //MATCH BETWEEN -VISIBLE COLS- AND -OK COLUMNS-
            boolean access = false;
            if (countV == countO) {
                  access = true;
                  System.out.println(CC.GREEN + "YES" + CC.RESET);
            } else {
                  access = false;
                  System.out.println(CC.RED + "NO" + CC.RESET);
            }

            //++++++++++++++++++++++++++++++++++++++++++++
            //MISSING -->COLUMNS WITH THE SAME NAME CONTROL<--
            Object[][] dst_tg = DST_TGControl(countV);
            Object[][] tb_ck = TB_CKControl(countV);

            String Dist = getValue("Dist", dst_tg[0][0].toString(), (int) dst_tg[0][1]);
            String Tabl = getValue("Tabl", tb_ck[0][0].toString(), (int) tb_ck[0][1]);
            String Tag = getValue("Tag", dst_tg[1][0].toString(), (int) dst_tg[1][1]);
            String Clock = getValue("Clock", tb_ck[1][0].toString(), (int) tb_ck[1][1]);

            if (access == true) {
                  mc.CreateTable(VC_R.getJTFS()[0].getText(), countV);
                  mc.InsertTable();
            }
      }

      private void TFSControl(int cv) {
            countO = 0;
            //IF EACH JTF HAS TEXT AND THE RIGHT CHARS
            for (int a = 0; a < cv; a++) {
                  if (!VC_R.getJTFS()[a].getText().isEmpty()) {
                        if (!VC_R.getJTFS()[a].getForeground().equals(Color.RED)) {

                              if (a == 0) {
                                    //IF THE TABLE ALREADY EXIST
                                    tableControl();

                              } else {
                                    //IF THE COLUMNS ARE OK
                                    countO++;
                              }
                        }
                  }
            }
      }

      private void tableControl() {
            boolean match = false;
            for (int b = 0; b < DT.getList_T().size(); b++) {
                  if (VC_R.getJTFS()[0].getText().equalsIgnoreCase(
                          DT.getList_T().get(b))) {
                        match = true;
                  } else {
                        if (match != true) {
                              match = false;
                        }
                  }
            }
            System.out.print("\t\tTable ");
            if (match == false) {
                  System.out.println(CC.GREEN + "OK" + CC.RESET);
                  countO++;
            } else {
                  System.out.println(CC.RED + "ALREADY EXIST" + CC.RESET);
            }
      }

      //+++++++++++++++++++++++++++++++++++++
      private Object[][] DST_TGControl(int cv) {
            int dstCount = 0;
            int tgCount = 0;

            ArrayList<Integer> dstCol = new ArrayList<Integer>();
            ArrayList<Integer> tgCol = new ArrayList<Integer>();
            //++++++++++++++++++++++++++++++++++++++++++++++++
            for (int a = 0; a < cv - 1; a++) {
                  if (VC_R.getBTNS_Dist()[a].isSelected()) {
                        dstCount++;
                        dstCol.add(a + 2);
                  }
                  if (VC_R.getBTNS_Tag()[a].isSelected()) {
                        tgCount++;
                        tgCol.add(a + 2);
                  }
            }
            //CREATING STRING DIST
            String Dist = "X" + Integer.toString(dstCount) + ": ";
            if (dstCount != 0) {
                  for (int a = 0; a < dstCount; a++) {
                        if (a == 0) {
                              Dist += dstCol.get(a).toString();
                        } else {
                              Dist += "_" + dstCol.get(a).toString();
                        }
                  }
            }

            String Tag = "X" + Integer.toString(tgCount) + ": ";
            if (tgCount != 0) {
                  for (int a = 0; a < tgCount; a++) {
                        if (a == 0) {
                              Tag += tgCol.get(a).toString();
                        } else {
                              Tag += "_" + tgCol.get(a).toString();
                        }
                  }
            }

            return new Object[][]{{Dist, dstCount}, {Tag, tgCount}};
      }

      private Object[][] TB_CKControl(int cv) {
            int tbCount = 0;
            int ckCount = 0;

            String Tabl = "C";
            String Clock = "C";
            for (int a = 0; a < cv - 1; a++) {
                  if (VC_R.getBTNS_Tabl()[a].isSelected()) {
                        Tabl += (a + 2);
                        tbCount++;
                  }
                  if (VC_R.getBTNS_Clock()[a].isSelected()) {
                        Clock += (a + 2);
                        ckCount++;
                  }
            }

            return new Object[][]{{Tabl, tbCount}, {Clock, ckCount}};
      }

      //++++++++++++++++++++++++++++++++++++++++
      private String getValue(String DST, String DSTV, int C) {
            System.out.print("\t" + DST + ": ");
            if (C != 0) {
                  System.out.println(CC.GREEN + DSTV + CC.RESET);
                  return DSTV;
            } else {
                  System.out.println(CC.RED + NS + CC.RESET);
                  return NS;
            }
      }

      private void printAllValues() {

      }
}