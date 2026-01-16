package com.university.contractors.model;

import com.google.common.base.Objects;

import javax.persistence.*;
import java.util.Date;

@Entity(name = "orders")
public class Order implements IdEntity<Long> {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id_order")
    private Long id;

    private String orderNumber;
    private Date orderDate;
    private Date relatedDate;
    private String note;

    @ManyToOne
    @JoinColumn(name = "ref_type_order")
    private OrderType orderType;

    @Override
    public Long getId() {
        return id;
    }

    @Override
    public void setId(Long id) {
        this.id = id;
    }

    public String getOrderNumber() {
        return orderNumber;
    }

    public void setOrderNumber(String orderNumber) {
        this.orderNumber = orderNumber;
    }

    public Date getOrderDate() {
        return orderDate;
    }

    public void setOrderDate(Date orderDate) {
        this.orderDate = orderDate;
    }

    public Date getRelatedDate() {
        return relatedDate;
    }

    public void setRelatedDate(Date relatedDate) {
        this.relatedDate = relatedDate;
    }

    public String getNote() {
        return note;
    }

    public void setNote(String note) {
        this.note = note;
    }

    public OrderType getOrderType() {
        return orderType;
    }

    public void setOrderType(OrderType orderType) {
        this.orderType = orderType;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Order order = (Order) o;
        return Objects.equal(id, order.id);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id);
    }
}