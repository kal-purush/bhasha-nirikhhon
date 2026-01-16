package sg.edu.nus.iss.vmcs.maintenance;

/*
 * Copyright 2003 ISS. The contents contained in this document may not be
 * reproduced in any form or by any means, without the written permission of
 * ISS, other than for the purpose for which it has been supplied.
 */

import java.awt.*;
import java.awt.event.*;

/**
 *
 *
 * @version 3.0 5/07/2003
 * @author Olivo Miotto, Pang Ping Li
 */

public class CoinLabelItem extends Panel {

    final public static int DEFAULT_LEN = 5;
    public final static int FLOW = 1;
    public final static int GRID = 2;
    public final static int QTY = 1;
    public final static int AMT = 2;

    public final static Color DT_COLOR = Color.darkGray;

    public final static Color ACT_COLOR = Color.white;

    private Label label;
    private Label qty;
    private Label amt;

    public CoinLabelItem(String bn, int l, int lt) {

        label = new Label(bn);
        qty = new Label("            ", Label.RIGHT);
        amt = new Label("            ", Label.RIGHT);

        qty.setBackground(Color.white);
        amt.setBackground(Color.white);

        if (lt == FLOW)
            this.setLayout(new FlowLayout());
        else if (lt == GRID)
            this.setLayout(new GridLayout(1, 3, 3, 3));

        this.add(label);
        this.add(qty);
        this.add(amt);
    }

    public void setActive(boolean st) {
        label.setEnabled(st);
        qty.setEnabled(st);
        amt.setEnabled(st);
    }

    public void setTextBackground(Color cl) {
        qty.setBackground(cl);
        amt.setBackground(cl);
    }

    public void clear() {
        qty.setText("");
        amt.setText("");
    }

    public void setValue(int type, int vl) {
        String sqt;
        sqt = String.valueOf(vl);

        switch (type) {
            case QTY:
                qty.setText(sqt);
                break;
            case AMT:
                amt.setText(sqt);
                break;
        }
    }

    public void setValue(int type, String vl) {
        switch (type) {
            case QTY:
                qty.setText(vl);
                break;
            case AMT:
                amt.setText(vl);
                break;
        }
    }
}