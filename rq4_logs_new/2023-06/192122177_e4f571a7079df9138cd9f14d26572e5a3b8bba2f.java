package org.frank.java.jackson.beans;

import com.fasterxml.jackson.annotation.JsonFormat;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;
import org.frank.java.jackson.utils.JsonDateFormatFull;

import java.util.Date;

public class Manager {
    
    private String name;
    
//    @JsonSerialize(using = JsonDateFormatFull.class)    
//    @JsonFormat( pattern="yyyy-MM-dd HH:mm:ss",timezone = "GMT+8")
    private Date date;

    public Manager() {
    }

    public Manager(String name, Date date) {
        this.name = name;
        this.date = date;
    }

    public String getName() {
        return name;
    }

    public void setName(String name) {
        this.name = name;
    }

    public Date getDate() {
        return date;
    }

    public void setDate(Date date) {
        this.date = date;
    }

    @Override
    public String toString() {
        return "Manager{" +
                "name='" + name + '\'' +
                ", date=" + date +
                '}';
    }
}