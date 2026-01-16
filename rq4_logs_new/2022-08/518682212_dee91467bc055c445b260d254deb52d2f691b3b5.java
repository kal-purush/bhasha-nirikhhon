package com.distributedSystems.ReplicationJarProject.Responses;

import java.io.Serializable;

public class ProductResponse  implements Serializable  {
    int number;
    String type;

    public ProductResponse(int number, String type) {
        this.number = number;
        this.type = type;
    }

    public int getNumber() {
        return number;
    }

    public void setNumber(int number) {
        this.number = number;
    }

    public String getType() {
        return type;
    }

    public void setType(String type) {
        this.type = type;
    }

    @Override
    public String toString() {
        return "{ number: "+number+", \n type: "+type+" }";
    }
}