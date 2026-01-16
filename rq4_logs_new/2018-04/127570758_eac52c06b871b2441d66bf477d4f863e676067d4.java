/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package Logica;

import java.util.ArrayList;

/**
 *
 * @author gojor
 */
public class Despacho {
    
    private String dpi;
    private String nombre;
    private int idDESPACHO;
    private ArrayList<Receta> list;
    
    public Despacho(int id, String name, String cui){
        this.idDESPACHO = id;
        this.nombre = name;
        this.dpi = cui;
    }
    
    public void getRecetas(){
        
    }

    public String getDpi() {
        return dpi;
    }

    public void setDpi(String dpi) {
        this.dpi = dpi;
    }

    public String getNombre() {
        return nombre;
    }

    public void setNombre(String nombre) {
        this.nombre = nombre;
    }

    public int getIdDESPACHO() {
        return idDESPACHO;
    }

    public void setIdDESPACHO(int idDESPACHO) {
        this.idDESPACHO = idDESPACHO;
    }

    public ArrayList<Receta> getList() {
        return list;
    }

    public void setList(ArrayList<Receta> list) {
        this.list = list;
    }
    
}