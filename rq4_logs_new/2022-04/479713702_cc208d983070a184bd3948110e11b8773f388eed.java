/*
 * Click nbfs://nbhost/SystemFileSystem/Templates/Licenses/license-default.txt to change this license
 * Click nbfs://nbhost/SystemFileSystem/Templates/JSF/JSFManagedBean.java to edit this template
 */
package controller;

import dao.NewsLetterDao;
import entity.NewsLetter;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Named;
import jakarta.enterprise.context.SessionScoped;
import java.io.Serializable;
import java.util.List;
/**
 *
 * @author mabea
 */
@Named(value = "newsLetterBean")
@SessionScoped
public class NewsLetterBean {

    private NewsLetter entity;
    private NewsLetterDao dao;
    private List<NewsLetter> list;
    
    
    public NewsLetterBean() {
    }
    public String getTitle(int id){
        NewsLetter c = this.getDao().findById(id);
        return c.getMail();
    }
    public void clear(){
        entity = new NewsLetter();
    }
    public void create(){
        this.getDao().create(entity);
        entity = new NewsLetter();
    }
    public void delete(NewsLetter c){
        this.getDao().delete(c);
        entity = new NewsLetter();
    }
    public void update(){
        this.getDao().update(entity);
        entity = new NewsLetter();
    }
    public NewsLetter getEntity() {
        if(entity == null) {
            entity = new NewsLetter();
        }
        return entity;
    }

    public void setEntity(NewsLetter entity) {
        this.entity = entity;
    }

    public NewsLetterDao getDao() {
        if(dao == null) {
            dao = new NewsLetterDao();
        }
        return dao;
    }

    public void setDao(NewsLetterDao dao) {
        this.dao = dao;
    }

    public List<NewsLetter> getList() {
        this.list = this.getDao().getList();
        return list;
    }

    public void setList(List<NewsLetter> list) {
        this.list = list;
    }
}