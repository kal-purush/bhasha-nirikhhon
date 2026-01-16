package techpark.game.avatar;

import techpark.game.Config;

import java.util.*;

import static java.lang.Math.abs;

/**
 * Created by Варя on 27.04.2017.
 */
@SuppressWarnings("ClassNamingConvention")
public class AOE {
    private Map<Square, ArrayList<Tower>> towersOnRoute;
    private LinkedList<Enemy> enemies;

    public AOE(int wave, List<Square> route, Map<Square, Character> avaliableGems) {
        this.towersOnRoute = new LinkedHashMap<>();
        this.enemies = new LinkedList<>();
        reorgonizeTowers(route, avaliableGems);
        for (int i = 0; i < Config.NUMBERENEMY; i++) {
            this.enemies.add(new Enemy(Config.ENEMYHP * Config.WAVECOFF * wave,
                    i, Config.ENEMYSPEED * Config.WAVECOFF * wave));
        }
    }

    private void reorgonizeTowers(List<Square> route, Map<Square, Character> gems) {
        for (Square routeSquare : route) {
            searchTower(routeSquare, gems);
        }
    }

    private void searchTower(Square route, Map<Square, Character> gems) {
        final ArrayList<Tower> towersAvailable = new ArrayList<>();
        for (Map.Entry<Square, Character> gem: gems.entrySet()){
            final Tower tower = new Tower(gem.getValue(), gem.getKey());
            if(abs(gem.getKey().getX() - route.getX()) <= tower.getRadius() &&
                    abs(gem.getKey().getY() - route.getY()) <= tower.getRadius()){
                towersAvailable.add(tower);
            }
        }
        if(!towersAvailable.isEmpty()) {
            towersOnRoute.put(route, towersAvailable);
        }
    }

    public List<EnemyDamage> calulateEnemyDamage() {
        final Map<Tower, Double> timeLeft = new HashMap<>();
        final List<EnemyDamage> enemyDamage = new ArrayList<>();
        for (Map.Entry<Square, ArrayList<Tower>> entry : towersOnRoute.entrySet()) {
            for (Tower tower : entry.getValue()) {
                damagePerTower(timeLeft, enemyDamage, tower);
                if (enemies.isEmpty())
                    break;
            }
            if (enemies.isEmpty())
                break;
        }
        return enemyDamage;
    }

    private void damagePerTower(Map<Tower, Double> timeLeft, List<EnemyDamage> enemyDamage, Tower tower) {
        final Double time = timeLeft.get(tower) == null ? 0.0 : timeLeft.get(tower);
        final Double speed = enemies.get(0).getSpeed();
        final double coff = tower.getSquare().isDiagonal() ? 1.44 : 1;
        for (int j = 0 ; j < enemies.size(); j++) {
            for (double i = time; i <= enemies.get(j).getSpeed()*coff; i += tower.getFrequency()) {
                enemies.get(j).incrementHp(tower.getDamage());
                enemyDamage.add(new EnemyDamage(tower.getSquare().getX() + i,
                        tower.getSquare().getY() + i, enemies.get(j)));
                if (enemies.get(j).getHp() <= 0.0) {
                    enemies.get(j).setDead();
                    enemies.remove(enemies.get(j--));
                    break;
                }
                timeLeft.putIfAbsent(tower, i - speed);
            }
        }
    }

    @SuppressWarnings("MagicNumber")
    public List<ThroneDamage> calulateThroneDamage() {
        final List<ThroneDamage> throneDamages = new ArrayList<>();
        Double throneHP = 100.0;
        for (Enemy enemy : enemies) {
            throneHP -= enemy.getHp() * Config.THRONECOFF;
            throneDamages.add(new ThroneDamage(throneHP, enemy.getNumber()));
            if (throneHP <= 0.0)
                break;
        }
        return throneDamages;
    }

    public LinkedList<Enemy> getEnemies() {
        return enemies;
    }
}