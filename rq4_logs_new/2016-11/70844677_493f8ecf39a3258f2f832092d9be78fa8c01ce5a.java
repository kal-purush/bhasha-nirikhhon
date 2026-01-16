package com.exigenservices.lectures.model;

import java.io.Serializable;

/**
 * Bookmark
 * Created by bolbin
 * on 07.11.2014.
 */
public class Bookmark implements Serializable{

    private static final long serialVersionUID = -2945844528912688113L;

    private String id;
    private String name;
    private String url;

    /**
     * Class constructor
     * @param id bookmark id
     * @param name name of bookmark
     * @param url url of bookmark
     */
    public Bookmark(String id, String name, String url) {
        this.id = id;
        this.name = name;
        this.url = url;
    }

    public String getId() {
        return id;
    }

    public String getName() {
        return name;
    }

    public String getUrl() {
        return url;
    }

    public void setId(String id) {
        this.id = id;
    }

    public void setName(String name) {
        this.name = name;
    }

    public void setUrl(String url) {
        this.url = url;
    }
}