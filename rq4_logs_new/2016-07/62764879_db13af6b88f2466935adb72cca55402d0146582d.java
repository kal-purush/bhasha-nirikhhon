/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package bean;

import dao.PaisDao;
import imp.PaisDaoimp;
import java.util.ArrayList;
import java.util.List;
import javax.annotation.PostConstruct;
import javax.enterprise.context.RequestScoped;
import javax.faces.bean.ManagedBean;
import javax.faces.model.SelectItem;
import model.Paises;
/**
 *
 * @author urzoi
 */
@ManagedBean
@RequestScoped
public class PaisBean {
     //private List<SelectItem> listPaises;
    private List<Paises> listPaises;
     private Paises pais;


    /*public List<SelectItem> getListPaises() {
        this.listPaises = new ArrayList<>();
        PaisDao pDao = new PaisDaoimp();
        List<Paises> p = pDao.listaPaises();
        listPaises.clear();
        for(Paises paises : p){
    SelectItem paisItem = new SelectItem(paises.getId(),paises.getNombre());
    this.listPaises.add(paisItem);
    }
        return listPaises;
    }*/

    
    public void setListPaises() {
        PaisDao pDao = new PaisDaoimp();
        this.listPaises = pDao.listaPaises();
    }

    public Paises getPais() {
        return pais;
    }

    public void setPais(Paises pais) {
        this.pais = pais;
    }

    public List<Paises> getListPaises() {
        return listPaises;
    }

    public void setListPaises(List<Paises> listPaises) {
        this.listPaises = listPaises;
    }
    
    /**
     * Creates a new instance of PaisBean
     */
    @PostConstruct
    public void init(){
        pais = new Paises();
        setListPaises();
    }
    
    public PaisBean() {
        //pais = new Paises();
    }
    
    public String guardar(){
        System.out.println("sistem out "+ pais.getNombre());
        return "index?faces-redirect=true";

    }
    
}