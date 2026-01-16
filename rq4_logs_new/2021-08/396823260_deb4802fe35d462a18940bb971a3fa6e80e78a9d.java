package homework.lab3;

import homework.lab3.hero.Hero;
import homework.lab3.history.FightHistory;
import homework.lab3.history.FightResultStatus;
import homework.lab3.history.FightState;

public class FightHistoryPrinter {

    public void printHistory(FightHistory fightHistory) {
        for (FightState fightState : fightHistory.getFightStates()) {
            Hero attacker = fightState.getAttacker();
            Hero defender = fightState.getDefender();

            System.out.println(attacker.getType() + " " + attacker.getName() + " kicked " + defender.getType() + " " +
                    defender.getName() + "for " + fightState.getHp() + "hp and " + fightState.getPower() + " power");
        }

        if (fightHistory.getResultStatus() == FightResultStatus.NOBODY_WON) {
            System.out.println("Nobody won");
        } else if (fightHistory.getResultStatus() == FightResultStatus.HERO1_WON) {
            printWinner(fightHistory.getHero1());
        } else {
            printWinner(fightHistory.getHero2());
        }
    }

    private void printWinner(Hero winner) {
        System.out.println(winner.getType() + " " + winner.getName() + " won the battle");
    }
}