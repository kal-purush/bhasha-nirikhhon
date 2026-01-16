package com.handh.com.materialdesign.model;

/**
 * Created by fallag113 on 6/28/17.
 */

public class TabTitle
{
    /*******************variable*******************/
    private Integer icon;
    private String title;

    /***************constructors*******************/
    public TabTitle(Integer icon, String title)
    {
        this.icon = icon;
        this.title = title;
    }

    public TabTitle()
    {
    }

    /***************setters*****************/
    public void setIcon(Integer icon) {
        this.icon = icon;
    }

    public void setTitle(String title) {
        this.title = title;
    }

    /**************getters******************/
    public Integer getIcon() {
        return this.icon;
    }

    public String getTitle() {
        return this.title;
    }
}