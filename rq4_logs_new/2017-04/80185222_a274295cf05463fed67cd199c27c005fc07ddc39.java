package com.example.lexlevi.sweapp.Models;

import com.google.gson.annotations.Expose;
import com.google.gson.annotations.SerializedName;

import java.io.Serializable;
import java.util.Date;

/**
 * Created by sphota on 4/23/17.
 */

public class Event implements Serializable {

    @SerializedName("_id")
    @Expose
    private String id;
    @SerializedName("name")
    @Expose
    private String name;
    @SerializedName("group")
    @Expose
    private String group; // id
    @SerializedName("completed")
    @Expose
    private Boolean completed;
    @SerializedName("dueDate")
    @Expose
    private Date dueDate;

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public String getGroup() {
        return group;
    }

    public void setGroup(String group) {
        this.group = group;
    }

    public String getId() {
        return id;
    }

    public void setId(String id) {
        this.id = id;
    }

    public Boolean getCompleted() { return completed; }

    public void setCompleted(Boolean completed) { this.completed = completed; }

    public Date getDueDate() { return dueDate; }

    public void setDueDate(Date dueDate) { this.dueDate = dueDate; }

    @Override
    public String toString() {
        return this.name;
    }
}