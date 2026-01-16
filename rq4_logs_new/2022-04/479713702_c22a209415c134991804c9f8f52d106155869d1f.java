
package controller;

import dao.AboutDao;
import entity.About;
import jakarta.enterprise.context.Dependent;
import jakarta.inject.Named;
import jakarta.enterprise.context.SessionScoped;
import java.io.Serializable;
import java.util.List;


@Named(value = "aboutBean")
@Dependent
public class AboutBean {
private About entity;
    private AboutDao dao;
    private List<About> list;
    
    
    public AboutBean() {
    }
    public String getTitle(int id){
        About c = this.getDao().findById(id);
        return c.getImage();
    }
    public void clear(){
        entity = new About();
    }
    public void create(){
        this.getDao().create(entity);
        entity = new About();
    }
    public void delete(About c){
        this.getDao().delete(c);
        entity = new About();
    }
    public void update(){
        this.getDao().update(entity);
        entity = new About();
    }
    public About getEntity() {
        if(entity == null) {
            entity = new About();
        }
        return entity;
    }

    public void setEntity(About entity) {
        this.entity = entity;
    }

    public AboutDao getDao() {
        if(dao == null) {
            dao = new AboutDao();
        }
        return dao;
    }

    public void setDao(AboutDao dao) {
        this.dao = dao;
    }

    public List<About> getList() {
        this.list = this.getDao().getList();
        return list;
    }

    public void setList(List<About> list) {
        this.list = list;
    }
    
}