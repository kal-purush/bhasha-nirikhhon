package Sweets;

/**
 * Создаем класс конфеток
 */
public class Candy extends Sweets {
    String sFactory;

    public Candy(String sName, float fCost, int iWeight, String sFactory) {
        super(sName, fCost, iWeight);
        this.sFactory = sFactory;
    }

    public String getsFactory() {
        return sFactory;
    }

    public void setsFactory(String sFactory) {
        this.sFactory = sFactory;
    }

    @Override
    public String toString(){
        return "Конфеты: " + super.toString() + " Производство: " + sFactory;
    }
}