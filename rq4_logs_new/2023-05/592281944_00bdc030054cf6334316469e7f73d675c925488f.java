package ua.ithillel.lesson23;

import ua.ithillel.lesson22.Hero;
import ua.ithillel.lesson22.HeroDao;

import java.util.List;

public class HeroServiceDao {
    private final HeroDao heroDao;

    public HeroServiceDao(HeroDao heroDao) {
        this.heroDao = heroDao;
    }

    public List<Hero> getHeroes() {
        return heroDao.findAll()
                .stream()
                .toList();
    }

    public Hero getByName(String nameHero){
        return heroDao.findByName(nameHero).stream()
                .findFirst().get();
    }

    public Hero getById(Long id){
        return heroDao.findById(id);
    }

    public Hero createHero(){
        Hero hero = new Hero("Cheburash2", "-", "Black", "Sovietist",
                "Brown", 41, "Dovzhenko", "Brown", "Good", 5);
        heroDao.create(hero);
        return hero;
    }

    public Hero update(){
        Hero hero = new Hero("Cheburash2", "-", "Brown", "Sovietist",
                "Yellow", 39, "Dovzhenko", "Brown", "Good", 5);
        heroDao.update(hero);
        return hero;
    }

    public boolean delete(Long id){
        return heroDao.delete(id);
    }

}
