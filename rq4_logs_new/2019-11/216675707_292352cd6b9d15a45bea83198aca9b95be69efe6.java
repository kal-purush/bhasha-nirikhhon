/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lp2.telegrammanager.modelsDAO;

import com.lp2.telegrammanager.interfaces.CRUD;
import com.lp2.telegrammanager.models.Category;
import java.util.ArrayList;

/**
 *
 * @author mathe
 */
public class CategoryDAO {
    private static String table = "categories";
    
    public static boolean save(Category category){
        String query = "INSERT INTO "+ table + "(name, description) values ('"+category.name+"', '"+ category.description+"')";
        return CRUD.create(query);
    }
    
    public static ArrayList<Category> getAll(){
        ArrayList<ArrayList<String>> result = CRUD.get("Select * from "+ table);
        
        ArrayList<Category> categories = new ArrayList<Category>();
        for(ArrayList<String> line: result){
            Category c = new Category(Integer.parseInt(line.get(0)), line.get(1),line.get(2));
            categories.add(c);
            
        }
        return categories;
    }

    public static Object getById(int id){
        String query = "Select * from "+table+" where id = "+id;
        ArrayList<ArrayList<String>> result = CRUD.get(query);
        ArrayList<Category> categories = new ArrayList<Category>();
        for(ArrayList<String> line: result){
            Category c = new Category(Integer.parseInt(line.get(0)), line.get(1),line.get(2));
            categories.add(c);
        }
        
        try{
            return categories.get(0);
        }catch(IndexOutOfBoundsException e){
            return null;
        }
    }
}