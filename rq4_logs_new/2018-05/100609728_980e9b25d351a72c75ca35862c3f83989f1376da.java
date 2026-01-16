package edu.eitc;

public class Berserker extends Warrior{
    boolean rage = false;

    @Override
    void attack(int range){
        if (withinAttackRange(range)) {
            if (rage) {
                System.out.println("Berserker attacks with RAGE!");
            }else{
                System.out.println("Berserker swings his fists at his opponent!");
            }
        }else{
            System.out.println("Berserker cannot reach his target!");
        }
    }

    void rage(){
        System.out.println("AHHH! RAGE!!");
        health -= 15;
        rage = true;
    }
}