package com.capstone.eCommercesite.model;

import javax.persistence.*;

// JOIN TABLE

@Entity(name = "orders_products")
public class OrdersProducts {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "orders_products_id")
    private int ordersProductsId;

    @ManyToOne(cascade = CascadeType.ALL)
    @JoinColumn(name = "order_id")
    private Order order;

    @ManyToOne(cascade = CascadeType.ALL)
    @JoinColumn(name = "product_id")
    private Product product;

    private int quantity;

    public OrdersProducts() {}

    public int getOrdersProductsId() {
        return ordersProductsId;
    }

    public void setOrdersProductsId(int ordersProductsId) {
        this.ordersProductsId = ordersProductsId;
    }

    public Order getOrder() {
        return order;
    }

    public void setOrder(Order order) {
        this.order = order;
    }

    public Product getProduct() {
        return product;
    }

    public void setProduct(Product product) {
        this.product = product;
    }

    public int getQuantity() {
        return quantity;
    }

    public void setQuantity(int quantity) {
        this.quantity = quantity;
    }
}