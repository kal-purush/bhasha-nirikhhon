package Sweets;

/**
 * Новый класс с печеньками
 */
public class Biscuit extends Sweets {
    String sForm;

    public Biscuit(String sName, float fCost, int iWeight, String sForm) {
        super(sName, fCost, iWeight);
        this.sForm = sForm;
    }

    public String getsForm() {
        return sForm;
    }

    public void setsForm(String sForm) {
        this.sForm = sForm;
    }

    @Override
    public String toString(){
        return "Конфеты: " + super.toString() + " Форма печенек: " + sForm;
    }
}