package Sweets;

/**
 * Собираем новогодний подарок. Общий класс сладостей.
 */
public abstract class Sweets {
    private String sName;
    private float fCost;
    private int iWeight;

    public Sweets(String sName, float fCost, int iWeight){
        this.sName = sName;
        this.fCost = fCost;
        this.iWeight = iWeight;
    }

    public String getsName() {
        return sName;
    }

    public void setsName(String sName) {
        this.sName = sName;
    }

    public float getfCost() {
        return fCost;
    }

    public void setfCost(float fCost) {
        this.fCost = fCost;
    }

    public int getiWeight() {
        return iWeight;
    }

    public void setiQuantity(int iWeight) {
        this.iWeight = iWeight;
    }

    @Override
    public String toString(){
        return "Сладость " + sName + ". Вес - " + iWeight + " грамм. Стоит " + fCost + " рублей";
    }
}