abstract class Fighter {

    boolean isVulnerable() {
        return false;
    }

    abstract int damagePoints(Fighter fighter);

}

class Warrior extends Fighter {

    private int damageP ;
    @Override
    public String toString() {
        return "Fighter is a Warrior";
    }

    @Override
    int damagePoints(Fighter fighter) {
        // return wizard.isVulnerable() ? 10 : 6;
        if(fighter.isVulnerable()){
            damageP = 10 ;
        }
        else {damageP = 6 ;}
        return  damageP ;
    }
}

class Wizard extends Fighter {
    private boolean vul = true ;
    private int DamageP ;

    @Override
    public String toString() {
        return "Fighter is a Wizard";
    }

    @Override
    boolean isVulnerable() {
        return vul;
    }

    @Override
    int damagePoints(Fighter fighter) {
        //return isVulnerable() ? 3 : 12;
        if (vul) {
            DamageP = 3 ;
        } else {
            DamageP = 12 ;
        }
        return  DamageP ;
    }

    void prepareSpell() {
        vul = false;
    }

}