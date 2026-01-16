package com.raisetech.inventoryapi.form;

import com.raisetech.inventoryapi.entity.InventoryProduct;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
public class CreateInventoryProductForm {
    private int productId;
    
    @NotNull
    @Min(1)
    private Integer quantity;

    public CreateInventoryProductForm(int productId, Integer quantity) {
        this.productId = productId;
        this.quantity = quantity;
    }

    public InventoryProduct convertToInventoryProductEntity() {
        InventoryProduct inventoryProduct = new InventoryProduct();
        inventoryProduct.setProductId(this.productId);
        inventoryProduct.setQuantity(this.quantity);
        return inventoryProduct;
    }
}