package com.deltorostudios.navigationjetpackeasysundaymorning;

public class CustomObject {

    int id;
    String name;
    String attribute;

    public CustomObject(int id, String name, String attribute) {
        this.id = id;
        this.name = name;
        this.attribute = attribute;
    }

    public int getId() {
        return id;
    }

    public void setId(int id) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getAttribute() {
        return attribute;
    }

    public void setAttribute(String attribute) {
        this.attribute = attribute;
    }
}