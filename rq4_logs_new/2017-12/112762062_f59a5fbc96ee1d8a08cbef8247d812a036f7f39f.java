package Sweets;

/**
 * Класс кексиков
 */
public class Cake extends Sweets {
    String sStuffing;

    public Candy(String sName, float fCost, int iWeight, String sStuffing) {
        super(sName, fCost, iWeight);
        this.sStuffing = sStuffing;
    }

    public String getsStuffing() {
        return sStuffing;
    }

    public void setsStuffing(String sStuffing) {
        this.sStuffing = sStuffing;
    }

    @Override
    public String toString(){
        return "Кексики: " + super.toString() + " Начинка: " + sStuffing;
    }
}