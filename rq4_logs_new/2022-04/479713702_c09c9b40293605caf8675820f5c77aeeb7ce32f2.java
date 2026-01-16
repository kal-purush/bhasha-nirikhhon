package controller;

import dao.CategoryDao;
import entity.Category;
import jakarta.enterprise.context.SessionScoped;
import jakarta.inject.Named;
import java.io.Serializable;
import java.util.List;

/**
 *
 * @author CASPER
 */
@Named(value="categoryBean")
@SessionScoped
public class CategoryBean implements Serializable{

    private Category entity;
    private CategoryDao dao;
    private List<Category> list;
    public CategoryBean() {
    }
    public void create(){
        this.getDao().create(entity);
        entity=new Category();
    }
    public void update(){
        this.getDao().update(entity);
        entity=new Category();
    }
    public void delete(Category c){
        this.getDao().delete(c);
        entity=new Category();
    }
    public void clear(){
        entity=new Category();
    }
    public String getName(int id){
        Category c=this.getDao().findById(id);
        return c.getName();
    }

    public Category getEntity() {
        return entity;
    }

    public void setEntity(Category entity) {
        this.entity = entity;
    }

    public CategoryDao getDao() {
        return dao;
    }

    public void setDao(CategoryDao dao) {
        this.dao = dao;
    }

    public List<Category> getList() {
        return list;
    }

    public void setList(List<Category> list) {
        this.list = list;
    }
    
    
}
