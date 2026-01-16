package HW;

public class Good {
    private String name;
    private int number;
    private int price;
    private int totalPrice;

    public Good(String name, int number, int price) {
        this.name = name;
        this.number = number;
        this.price = price;
        this.totalPrice = this.price * this.number;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setNumber(int number) {
        this.number = number;
        this.totalPrice = this.price * this.number;
    }

    public void setPrice(int price) {
        this.price = price;
        this.totalPrice = this.price * this.number;
    }

    public int getNumber() {
        return this.number;
    }

    public int getTotalPrice() {
        return this.totalPrice;
    }

    public int getPrice() {
        return this.price;
    }

    public String getName() {
        return this.name;
    }

    public void showInfo(){
        System.out.println("Название: " + this.name);
        System.out.println("Количество: " + this.number);
        System.out.println("Цена за единицу: " + this.price);
        System.out.println("Итоговая цена: " + this.totalPrice);
    }
}