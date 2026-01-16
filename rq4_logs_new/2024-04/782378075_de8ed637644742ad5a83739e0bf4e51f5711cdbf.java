public class Koshel {
    private String material;
    private String color;
    private int otsekov;
    private int money;

    public static void main(String[] args) {
           }

    @Override
    public String toString() {
        return "Кошелёк " +
                "материал'" + material + '\'' +
                ", цвет " + color + '\'' +
                ", кармашки " + otsekov +
                ", деньги" + money +
                ".";
    }

    public Koshel(String material, String color, int otsekov, int money) {
        this.material = material;
        this.color = color;
        this.otsekov = otsekov;
        this.money = money;
    }

    public void setMoney(int money) {
        this.money = money;
        if (money<0)
            money=0;
    }

    public String getMaterial() {
        return material;
    }

    public String getColor() {
        return color;
    }

    public int getOtsekov() {
        return otsekov;
    }
}