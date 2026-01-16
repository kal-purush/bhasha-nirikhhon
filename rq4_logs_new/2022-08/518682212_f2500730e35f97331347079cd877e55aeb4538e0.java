package com.distributedSystems.ReplicationJarProject.BackUpCoordinator;

import java.io.Serializable;

public class RestoreRequest implements Serializable{
    private int productA;
    private int productB;

    public RestoreRequest() {
        productA = -1;
        productB = -1;
    }

    public int getProductA() {
        return productA;
    }

    public int getProductB() {
        return productB;
    }

    public void setProductA(int productA) {
        this.productA = productA;
    }

    public void setProductB(int productB) {
        this.productB = productB;
    }

    public boolean requestValid() {
        if (productA == -1 && productB == -1) return false;
        else return true;
    }

    public boolean equals(RestoreRequest reference) {
        if (productA == reference.getProductA() && productB == reference.getProductB())
            return true;
        else
            return false;
    }
}