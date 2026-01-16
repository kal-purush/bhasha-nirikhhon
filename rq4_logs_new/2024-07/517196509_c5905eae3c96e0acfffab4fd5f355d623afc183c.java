package page.info.edit;

import common.CommonStatic;
import common.pack.PackData;
import common.util.stage.Stage;
import common.util.stage.StageLimit;
import page.JL;
import page.JTF;
import page.MainLocale;
import page.Page;

import javax.swing.*;

public class StageLimitTable extends Page {

    private static final long serialVersionUID = 1L;

    private static String[] climits;

    static {
        redefine();
    }

    protected static void redefine() {
        climits = Page.get(MainLocale.INFO, "ht2", 2);
    }

    private final JL bank = new JL(MainLocale.INFO, "ht20");
    private final JL cres = new JL(MainLocale.INFO, "ht21");
    private final JTF jban = new JTF();
    private final JTF jcre = new JTF();

    private final PackData.UserPack pac;

    private StageLimit stli;

    protected StageLimitTable(Page p, PackData.UserPack pack) {
        super(p);
        pac = pack;
        ini();
    }

    @Override
    protected void resized(int x, int y) {
        int w = 1400 / 8;

        set(bank, x, y, 0, 0, w, 50);
        set(jban, x, y, w, 0, w, 50);
        set(cres, x, y, w * 2, 0, w, 50);
        set(jcre, x, y, w * 3, 0, w, 50);
    }

    private void ini() {
        add(bank);
        add(jban);
        add(cres);
        add(jcre);
        addListeners();
    }

    public void setData(Stage st) {
        if (st == null) {
            return;
        }
        stli =  st.getCont().stageLimit == null ? st.getCont().stageLimit = new StageLimit() : st.getCont().stageLimit;
        jban.setText(stli.maxMoney + "");
        jcre.setText(stli.globalCooldown + "");
    }

    private void addListeners() {
        jban.setLnr(x -> stli.maxMoney = Math.min(CommonStatic.parseIntN(jban.getText()), 0));
        jcre.setLnr(x -> stli.globalCooldown = Math.min(CommonStatic.parseIntN(jcre.getText()), 0));
    }

    @Override
    protected JButton getBackButton() {
        return null;
    }
}