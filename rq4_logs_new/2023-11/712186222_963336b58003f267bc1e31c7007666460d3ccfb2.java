//ファイル名: Main2.java

import jp.ac.uryukyu.ie.e235743.*;

public class Main2 {
    public static void main(String[] args){
        Hero hero = new Warrior("勇者", 10, 5); // (a)
        Enemy enemy = new Enemy("スライム", 6, 3);

        System.out.printf("%s vs. %s\n", hero.getName(), enemy.getName());

        Hero hero2 = new Hero(null, 0, 0);
        System.out.println(hero2);
        
        int turn = 0;
        while( hero.isDead() == false && enemy.isDead() == false ){
            turn++;
            System.out.printf("%dターン目開始！\n", turn);
            hero.attack(enemy); // (b)
            enemy.attack(hero);
        }
        System.out.println("戦闘終了");
    }
}