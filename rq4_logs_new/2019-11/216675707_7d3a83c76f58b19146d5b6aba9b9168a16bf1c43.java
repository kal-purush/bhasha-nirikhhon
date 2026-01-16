/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.lp2.telegrammanager.modelsDAO;

import com.lp2.telegrammanager.interfaces.CRUD;
import com.lp2.telegrammanager.models.Place;
import com.lp2.telegrammanager.models.Property;
import com.lp2.telegrammanager.models.Category;
import java.util.ArrayList;
/**
 *
 * @author mathe
 */
public class PropertyDAO {
    private static String table = "properties";
    
    public static boolean save(Property property){
        String query = "INSERT INTO "+ table + "(code, name, description, place_id, category_id) VALUES ('"+
                property.code+"', '"+
                property.name+"', '"+
                property.description+"', "+
                property.place.id+", "+
                property.category.id+")";
        
        return CRUD.create(query);
    }
    
    public static ArrayList<Property> getAll(){
        ArrayList<ArrayList<String>> result = CRUD.get("Select * from "+ table);
        ArrayList<Property> properties = new ArrayList<Property>();
        for(ArrayList<String> line: result){
            Place place = (Place) PlaceDAO.getById(Integer.parseInt(line.get(4)));
            Category category = (Category) CategoryDAO.getById(Integer.parseInt(line.get(5)));
            Property p = new Property(Integer.parseInt(line.get(0)), line.get(1),line.get(2), line.get(3), place, category);
            properties.add(p);
        }
        return properties;
    }
    
    public static Object getById(int id){
        String query = "Select * from "+table+" where id = "+id;
        ArrayList<ArrayList<String>> result = CRUD.get(query);
        ArrayList<Property> properties = new ArrayList<Property>();
        for(ArrayList<String> line: result){
            Place place = (Place) PlaceDAO.getById(Integer.parseInt(line.get(4)));
            Category category = (Category) CategoryDAO.getById(Integer.parseInt(line.get(5)));
            Property p = new Property(Integer.parseInt(line.get(0)), line.get(1), line.get(2), line.get(3), place, category);
            properties.add(p);
        }
        try{
            return properties.get(0);
        }catch(IndexOutOfBoundsException e){
            return null;
        }
    }
    
    public static boolean update(Property property){
        String query = "UPDATE "+table+" SET code = '"+
            property.code+"', "+
            "name = '"+
            property.name+"', "+
            "description = '"+
            property.description+"', "+
            "place_id = "+
            property.place.id+", "+
            "category_id = "+
            property.category.id+
            " WHERE id = "+property.id;
        return CRUD.update(query);
    }
    
}