/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package compresion_rick;

/**
 *
 * @author RICARDO
 */
public class Arbin 
{
    //la interpretacion mas mediocre que pude hacer de un arbol, me siento orgulloso equisde v:
    public Arbin(Object info, Arbin superior, boolean orientation) 
    {
        this.info = info;
        this.superior = superior;
        this.orientation = orientation;
        
        if(orientation)
        {
            superior.lelft_arbin=this;
        }
        else
        {
            superior.rigth_arbin=this;
        }        
    }

    public Arbin(Object info) {
        this.info = info;
        this.superior = null;
        this.orientation = null;
    }

    public Arbin(Object info, Arbin rigth_arbin, Arbin lelft_arbin) {
        this.info = info;
        this.rigth_arbin = rigth_arbin;
        this.lelft_arbin = lelft_arbin;
    }

    //GETTERS & SETTERS_________________________________________________________
    
    public Object getInfo() {
        return info;
    }

    public void setInfo(Object info) {
        this.info = info;
    }

    public Boolean getOrientation() {
        return orientation;
    }

    public void setOrientation(Boolean orientation) {
        this.orientation = orientation;
    }

    public Arbin getRigth_arbin() {
        return rigth_arbin;
    }

    public void setRigth_arbin(Arbin rigth_arbin) {
        this.rigth_arbin = rigth_arbin;
    }

    public Arbin getLelft_arbin() {
        return lelft_arbin;
    }

    public void setLelft_arbin(Arbin lelft_arbin) {
        this.lelft_arbin = lelft_arbin;
    }

    public Arbin getSuperior() {
        return superior;
    }

    public void setSuperior(Arbin superior) {
        this.superior = superior;
    }
    
//ATRIB_________________________________________________________________________    
    
    Object  info;
    Boolean orientation;
    Arbin   rigth_arbin,
            lelft_arbin,
            superior;
}