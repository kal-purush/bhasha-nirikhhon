package modelos;

import lombok.Getter;
import lombok.Setter;

public class OrderList {
    @Getter
    @Setter
   private String ProductId;
    @Getter @Setter
    private int ProductQuantity;
    @Getter @Setter
    private double ProductPrice;
    @Getter @Setter
    private double Total;

    public OrderList(String ProductId, int ProductQuantity, double ProductPrice, double Total){
        this.ProductId = ProductId;
        this.ProductQuantity = ProductQuantity;
        this.ProductPrice = ProductPrice;
        this.Total = Total;
    }
}