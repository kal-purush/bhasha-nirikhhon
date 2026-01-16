package de.ait_tr.g_40_shop.domain.entity;

import jakarta.persistence.*;

import java.util.List;
import java.util.Objects;

@Entity
@Table(name = "cart")
public class Cart {
    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id")
    private Long id;

    private Customer customer;
    private List<Product> products;

    // TODO домашнее задание - функционал корзины

    public Long getId() {
        return id;
    }

    public void setId(Long id) {
        this.id = id;
    }

    public Customer getCustomer() {
        return customer;
    }

    public void setCustomer(Customer customer) {
        this.customer = customer;
    }

    public List<Product> getProducts() {
        return products;
    }

    public void setProducts(List<Product> products) {
        this.products = products;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof Cart cart)) return false;
        return Objects.equals(getId(), cart.getId()) && Objects.equals(getCustomer(), cart.getCustomer()) && Objects.equals(getProducts(), cart.getProducts());
    }

    @Override
    public int hashCode() {
        return Objects.hash(getId(), getCustomer(), getProducts());
    }

    @Override
    public String toString() {
        return String.format("Cart: id - %d, contains %d products",
                id, products == null ? 0 : products.size());
    }
}