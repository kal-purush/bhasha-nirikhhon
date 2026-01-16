package org.flowershop.domain.products;

public class Decoration extends Product {
    private String type;


    public Decoration(String name, double price, String type) {
        super(name, price);
        this.type = type;
    }


    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }


    // TODO: Method to display flower info on screen.
    @Override
    public void displayProduct() {
        // TODO: Logic to display a decoration product.
        final StringBuilder sb = new StringBuilder("Flower{");
        sb.append("name=").append(getName());
        sb.append("price=").append(getPrice());
        sb.append("type=").append(type);
        sb.append('}');
        System.out.println(sb);
    }
}