package com.university.contractors.model;

import com.google.common.base.Objects;

import javax.persistence.*;

@Entity(name = "types_orders")
public class OrderType implements IdEntity<Long> {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    @Column(name = "id_type_order")
    private Long id;
    private String typeOrderName;

    @Override
    public Long getId() {
        return id;
    }

    @Override
    public void setId(Long id) {
        this.id = id;
    }

    public String getTypeOrderName() {
        return typeOrderName;
    }

    public void setTypeOrderName(String typeOrderName) {
        this.typeOrderName = typeOrderName;
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        OrderType orderType = (OrderType) o;
        return Objects.equal(id, orderType.id);
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(id);
    }
}