package Listeners;

import First.VF_R;
import MC.DT;
import MC.notMyMethods;
import java.awt.Font;
import java.awt.event.ActionEvent;
import java.awt.event.ActionListener;

/**
 *
 * @author Christopher
 */
public class BTNS_TBActionListener implements ActionListener {

      notMyMethods n_mm = new notMyMethods();

      @Override
      public void actionPerformed(ActionEvent e) {
            Font f = VF_R.getJT().getFont();
            int fsize = f.getSize();
            int rheight = VF_R.getJT().getRowHeight();

            if (e.getSource() == VF_R.getBTN_PLUS()) {
                  System.out.println(VF_R.getBTN_PLUS().getName());
                  fsize++;
                  rheight++;
            } else if (e.getSource() == VF_R.getBTN_MINUS()) {
                  System.out.println(VF_R.getBTN_MINUS().getName());
                  fsize--;
                  rheight--;
            }

            VF_R.getJT().setFont(new Font(f.getName(), f.getSize(), fsize));
            VF_R.getJT().setRowHeight(rheight);
            VF_R.getTF_CE().setFont(new Font(f.getName(), f.getSize(), fsize));
            n_mm.rez(VF_R.getJT(), DT.autoState);

      }

}