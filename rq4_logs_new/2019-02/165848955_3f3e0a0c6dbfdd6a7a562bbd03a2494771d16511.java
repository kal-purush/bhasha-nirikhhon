package fr.esiea.supermarket.model.Discounts;

import fr.esiea.supermarket.model.*;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Bundle {

    private List<ProductQuantity> items = new ArrayList<>();
    private double price;


    public Bundle(double price){
        this.price=price;
    }
    public void addProducttoBendle (Product product,int quantity){
        ProductQuantity ri = new ProductQuantity(product,quantity);
        items.add(ri);
    }
    public  List<ProductQuantity> getItems(){
        return items;
    }
    public  double getPrice(){
        return price;
    }


}