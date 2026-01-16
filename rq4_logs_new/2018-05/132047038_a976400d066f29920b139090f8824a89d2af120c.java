package com.spitfire.forexmemo.domain;

import org.springframework.data.annotation.Id;

/**
 * Created by H.W. on 5/4/2018.
 */
public class SecreteModel {

    /**
     * Visitor's id.
     */
    @Id
    private String id;

    /**
     * Secrete associated with the visitor's token
     */
    private String secrete;

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getSecrete() {
        return secrete;
    }

    public void setSecrete(String secrete) {
        this.secrete = secrete;
    }
}