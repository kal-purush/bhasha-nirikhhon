package com.simple.generator.common;

import com.github.pagehelper.Page;

import java.io.Serializable;
import java.util.List;

public class PageList<T> implements Serializable {
    private Paginator paginator;
    private List<T> list;

    public PageList(List<T> list) {
        if(list instanceof Page){
            paginator=new Paginator((Page)list);
            this.list=list;
        }
    }

    public Paginator getPaginator() {
        return paginator;
    }

    public void setPaginator(Paginator paginator) {
        this.paginator = paginator;
    }

    public List<T> getList() {
        return list;
    }

    public void setList(List<T> list) {
        this.list = list;
    }



}