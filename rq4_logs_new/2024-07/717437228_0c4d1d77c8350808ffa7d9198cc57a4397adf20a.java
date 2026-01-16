package com.raisetech.inventoryapi.form;

import com.raisetech.inventoryapi.entity.InventoryProduct;
import jakarta.validation.constraints.Min;
import jakarta.validation.constraints.NotNull;
import lombok.Getter;

@Getter
public class UpdateInventoryProductForm {
    private int productId;

    @NotNull
    @Min(1)
    private Integer quantity;

    public UpdateInventoryProductForm(int productId, Integer quantity) {
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