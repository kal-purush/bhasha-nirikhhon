package homework.lab3;

import homework.lab3.hero.Hero;
import homework.lab3.history.FightHistory;

public class GameManager {

    public FightHistory fight(Hero hero1, Hero hero2) {
        FightHistory fightHistory = new FightHistory(hero1, hero2);

        while (hero1.isAlive() && hero2.isAlive()) {
            hero1.kick(hero2);
            fightHistory.addSnapshot(hero1, hero2);

            hero2.kick(hero1);
            fightHistory.addSnapshot(hero1, hero2);
        }

        return fightHistory;
    }
}