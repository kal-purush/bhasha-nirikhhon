package com.eiryu.nte.bean;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Created by eiryu on 2015/05/02.
 */
public class Tag {

    private String name;
    private int indentCount;
    private Map<String, String> attributes;
    private List<Tag> children;

    public Tag() {
        attributes = new LinkedHashMap<>();
        attributes.put("", "");
        children = new ArrayList<>();
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public int getIndentCount() {
        return indentCount;
    }

    public void setIndentCount(int indentCount) {
        this.indentCount = indentCount;
    }

    public Map<String, String> getAttributes() {
        return attributes;
    }

    public void setAttributes(Map<String, String> attributes) {
        this.attributes = attributes;
    }

    public List<Tag> getChildren() {
        return children;
    }

    public void setChildren(List<Tag> children) {
        this.children = children;
    }

    @Override
    public String toString() {
        return "Tag{" +
                "name='" + name + '\'' +
                ", indentCount=" + indentCount +
                ", attributes=" + attributes +
                ", children=" + children +
                '}';
    }
}