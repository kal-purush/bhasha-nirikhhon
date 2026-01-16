class Toko {
    String ID;
    String name;
    int stock;
    int price;

    public String getID() {
        return ID;
    }

    public void setID(String iD) {
        ID = iD;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getStock() {
        return stock;
    }

    public void setStock(int stock) {
        this.stock = stock;
    }

    public int getPrice() {
        return price;
    }

    public void setPrice(int price) {
        this.price = price;
    }

    public void productData() {
        System.out.println("ID Product\t: " + getID());
        System.out.println("Name Product\t: " + getName());
        System.out.println("Price Product\t: " + getPrice());
        if (getStock() <= 0) {
            System.out.println("Stock Prouct\t: Product Not Avaliable");
        } else {
            System.out.println("Stock Products\t: " + getStock());
        }
    }

    public void restProduct() {
        System.out.println("------------------------------------------------");
        if (getStock() <= 0) {
            System.out.printf("Product %s Not Avaliable",name);
        } else {
            System.out.println("Stock Products : " + getStock());
            System.out.printf("Products %s Avaliable",name);
        }
        System.out.println("\n------------------------------------------------");
    }
}

public class TP2_2_H071231048 {
    public static void main(String[] args) {
        Toko cahaya = new Toko();
        cahaya.ID = "H071231050";
        cahaya.name = "Apel";
        cahaya.price = 4000;
        cahaya.stock = -1;


        cahaya.productData();
        cahaya.restProduct();
    }
}