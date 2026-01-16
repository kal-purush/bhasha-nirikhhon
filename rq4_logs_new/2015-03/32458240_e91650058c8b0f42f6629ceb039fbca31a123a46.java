/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.pepe.controller;

import com.pepe.jpa.entities.EtapaPractica;
import com.pepe.jpa.sesions.EtapaPracticaFacade;
import java.util.ArrayList;
import java.util.List;
import javax.ejb.EJB;
import javax.faces.application.FacesMessage;
import javax.faces.bean.ManagedBean;
import javax.faces.bean.SessionScoped;
import javax.faces.component.UIComponent;
import javax.faces.context.FacesContext;
import javax.faces.convert.Converter;
import javax.faces.convert.FacesConverter;

/**
 *
 * @author Aprendiz
 */
@ManagedBean
@SessionScoped
public class PracticasController {
EtapaPractica etapaPracticaActual;
@EJB
private EtapaPracticaFacade etapaPracticaFacade;
private List<EtapaPractica> listaEtapaPractica=null;
    /**
     * Creates a new instance of PracticasController
     */
    public PracticasController() {
    }

    public EtapaPractica getEtapaPracticaActual() {
        if (etapaPracticaActual == null) {
            etapaPracticaActual = new EtapaPractica();
        }

        return etapaPracticaActual;
    }

    public void setEtapaPracticaActual(EtapaPractica etapaPracticaActual) {
        this.etapaPracticaActual = etapaPracticaActual;
    }

    public EtapaPracticaFacade getEtapaPracticaFacade() {
        return etapaPracticaFacade;
    }

    public List<EtapaPractica> getListaEtapaPractica() {
         if (listaEtapaPractica == null) {
            try {
                listaEtapaPractica=getEtapaPracticaFacade().findAll();
            } catch (Exception e) {
                addErrorMessage("error cerrando el recurso"+e.getClass().getName(),"mensaje: "+ e.getMessage());
            }
   
        }
        return listaEtapaPractica;
    }
       private void recargarLista(){
      listaEtapaPractica=null;
    }
  public String prepareCreate() {
        etapaPracticaActual = new EtapaPractica();
       listaEtapaPractica = new ArrayList<>();
        return "/admin/aprendices/crear_etapa_practica";
    }

    public String prepareEdit() {
        return "/admin/Editar_etapa_practica";
    }

    public String prepareView() {
        return "/admin/Ver_etapa_practica";
    }
    
     public String prepareList() {
        recargarLista();
        return "/admin/List";
    }
  public String addEtapaPractica() {
        try {
            getEtapaPracticaFacade().create(etapaPracticaActual);
            addSuccessMessage("Crear usuarios", "usuarios Creados Exitosamente");
            recargarLista();
            return "/admin/aprendices/Ver_etapa_practica";
        } catch (Exception e) {
            addErrorMessage("Error closing resource " + e.getClass().getName(), "Message: " + e.getMessage());
            return null;
        }
  }
  

  
  
  public String updateEtapaPractica() {
        try {
          getEtapaPracticaFacade().edit(etapaPracticaActual);
            addSuccessMessage("Actualizar etapa paractica", "Etapa preactica Actualizada Exitosamente");
            recargarLista();
            return "/admin/aprendices/Ver_etapa_practica";
        } catch (Exception e) {
            addErrorMessage("Error closing resource " + e.getClass().getName(), "Message: " + e.getMessage());
            return null;
        }
    }
   public String deleteEtapaPractica() {
        try {
            getEtapaPracticaFacade().remove(etapaPracticaActual);
            addSuccessMessage("Eliminar etapa practica", "etapa practica Eliminado Exitosamente");
            recargarLista();
        } catch (Exception e) {
            addErrorMessage("Error closing resource " + e.getClass().getName(), "Message: " + e.getMessage());
        }
        return "/admin/aprendices/List";
    }

    private void addErrorMessage(String title, String msg) {
        FacesMessage facesMsg
                = new FacesMessage(FacesMessage.SEVERITY_ERROR, title, msg);
        FacesContext.getCurrentInstance().addMessage(null, facesMsg);
    }

    private void addSuccessMessage(String title, String msg) {
        FacesMessage facesMsg
                = new FacesMessage(FacesMessage.SEVERITY_INFO, title, msg);
        FacesContext.getCurrentInstance().addMessage("successInfo", facesMsg);
    }
    
}