package homework.lab3.hero;

import com.github.javafaker.Faker;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Random;
import java.util.function.Function;

public class HeroFactory {
    private final List<Function<String, Hero>> heroCreators = new ArrayList<>();
    private final Random random = new Random();
    private final Faker faker = new Faker();

    public HeroFactory() {
        heroCreators.add(Hobbit::new);
        heroCreators.add(Elf::new);
        heroCreators.add(King::new);
        heroCreators.add(Knight::new);
    }

    public Hero createHero() {
        int heroRandomIndex = random.nextInt(heroCreators.size());

        return heroCreators.get(heroRandomIndex).apply(faker.gameOfThrones().character());
    }
}