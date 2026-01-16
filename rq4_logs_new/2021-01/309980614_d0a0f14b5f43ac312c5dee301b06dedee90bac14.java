package com.java15.example.collections;

import java.util.Objects;

public class Customer implements Comparable{

    private int id;
    private String name;

    public Customer(int id, String name) {
        this.id = id;
        this.name = name;
    }

    public int getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    @Override
    public String toString() {
        return "Customer{" +
                "id=" + id +
                ", name='" + name + '\'' +
                '}';
    }

  /*  @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;
        Customer customer = (Customer) o;
        return id == customer.id && Objects.equals(name, customer.name);
    }

    @Override
    public int hashCode() {
        return Objects.hash(id, name);
    }*/

    @Override
    public int compareTo(Object o) {
        Objects.requireNonNull(o,"Requires Customer class.");
        if(!(o instanceof Customer customer))
            throw new IllegalArgumentException("Object is not instance of Customer");
        if(this.id == customer.id && this.name.equals(customer.getName()))
            return 0;
        else if (this.id >= customer.id && 1 == this.name.compareTo(customer.getName()))
            return 1;
        else
            return -1;
    }
}