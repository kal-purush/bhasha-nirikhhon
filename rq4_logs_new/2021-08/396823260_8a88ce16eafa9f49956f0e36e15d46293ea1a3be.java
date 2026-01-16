package homework.lab3.history;

import homework.lab3.hero.Hero;

import java.util.ArrayList;
import java.util.List;

public class HeroFightHistory {
    private final String heroName;
    private final String heroType;

    private final List<HeroState> heroStates = new ArrayList<>();

    public HeroFightHistory(Hero hero) {
        heroName = hero.getName();
        heroType = hero.getType();

        addHeroSnapshot(hero);
    }

    public void addHeroSnapshot(Hero hero) {
        heroStates.add(new HeroState(hero.getPower(), hero.getHp()));
    }
}