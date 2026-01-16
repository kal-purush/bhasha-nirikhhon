package com.example.prm_shop.models.request;

public class CartRequest {
    private int memberId;
    private int productId;
    private int quantity;
    private String size;

    public CartRequest(int memberId, int productId, int quantity, String size) {
        this.memberId = memberId;
        this.productId = productId;
        this.quantity = quantity;
        this.size = size;
    }

    public int getMemberId() {
        return memberId;
    }

    public void setMemberId(int memberId) {
        this.memberId = memberId;
    }

    public int getProductId() {
        return productId;
    }

    public void setProductId(int productId) {
        this.productId = productId;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }

    public String getSize() {
        return size;
    }

    public void setSize(String size) {
        this.size = size;
    }
}