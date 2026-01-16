/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/JSF/JSFManagedBean.java to edit this template
 */
package controller;

import dao.BlogDao;
import entity.Blog;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Named;
import jakarta.enterprise.context.SessionScoped;
import java.io.Serializable;
import java.util.List;

/**
 *
 * @author Gökhan
 */
@Named(value = "blogBean")
@Dependent
public class BlogBean {

   private Blog entity;
    private BlogDao dao;
    private List<Blog> list;
    
    
    public BlogBean() {
    }
    public String getTitle(int id){
        Blog c = this.getDao().findById(id);
        return c.getTitle();
    }
    public void clear(){
        entity = new Blog();
    }
    public void create(){
        this.getDao().create(entity);
        entity = new Blog();
    }
    public void delete(Blog c){
        this.getDao().delete(c);
        entity = new Blog();
    }
    public void update(){
        this.getDao().update(entity);
        entity = new Blog();
    }
    public Blog getEntity() {
        if(entity == null) {
            entity = new Blog();
        }
        return entity;
    }

    public void setEntity(Blog entity) {
        this.entity = entity;
    }

    public BlogDao getDao() {
        if(dao == null) {
            dao = new BlogDao();
        }
        return dao;
    }

    public void setDao(BlogDao dao) {
        this.dao = dao;
    }

    public List<Blog> getList() {
        this.list = this.getDao().getList();
        return list;
    }

    public void setList(List<Blog> list) {
        this.list = list;
    }
    
}