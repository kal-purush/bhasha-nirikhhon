package theClanless.actions;

import com.megacrit.cardcrawl.actions.AbstractGameAction;
import com.megacrit.cardcrawl.actions.common.ApplyPowerAction;
import com.megacrit.cardcrawl.actions.common.MakeTempCardInDrawPileAction;
import com.megacrit.cardcrawl.actions.common.MakeTempCardInHandAction;
import com.megacrit.cardcrawl.cards.AbstractCard;
import com.megacrit.cardcrawl.characters.AbstractPlayer;
import com.megacrit.cardcrawl.core.Settings;
import com.megacrit.cardcrawl.dungeons.AbstractDungeon;
import theClanless.cards.core.QuickJab;
import theClanless.powers.AdditionalStrikePower;

import java.util.ArrayList;
import java.util.concurrent.ThreadLocalRandom;

public class BumsRushAction extends AbstractGameAction {

    private AbstractPlayer player;
    private ArrayList<AbstractCard> cardList;
    private AbstractCard card = new QuickJab();
    private boolean applyPower;
    private int amount = 0;


    public BumsRushAction(AbstractPlayer p, ArrayList<AbstractCard> cardList) {
        this.duration = Settings.ACTION_DUR_XFAST;
        this.actionType = ActionType.WAIT;
        this.player = p;
        this.cardList = cardList;
    }

    public BumsRushAction(AbstractPlayer p, AbstractCard card) {
        this.duration = Settings.ACTION_DUR_XFAST;
        this.actionType = ActionType.WAIT;
        this.player = p;
        this.cardList = new ArrayList<>();
        this.cardList.add(card);
    }


    @Override
    public void update() {
        int randomNum = 0;
        if (cardList.size() > 1) {
            randomNum = ThreadLocalRandom.current().nextInt(0, cardList.size());
        }
        AbstractDungeon.actionManager.addToBottom(
                new MakeTempCardInDrawPileAction(cardList.get(randomNum), 1, true, true)
        );
        this.isDone = true;
    }
}