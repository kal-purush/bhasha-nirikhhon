package com.metafour.cwbay.model;

import java.util.List;

/**
 * Created by nadim on 1/26/15.
 */
public class Category {
    public static final String JSON_TAG_ID = "id";
    public static final String JSON_TAG_NAME = "name";
    public static final String JSON_TAG_CHILDREN = "children";

    private int id;
    private String name;
    private List<Category> children;

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

    public List<Category> getChildren() {
        return children;
    }

    public void setChildren(List<Category> children) {
        this.children = children;
    }

    @Override
    public String toString() {
        return "Category{" +
                "id=" + id +
                ", name='" + name + '\'' +
                ", children=" + children +
                '}';
    }
}