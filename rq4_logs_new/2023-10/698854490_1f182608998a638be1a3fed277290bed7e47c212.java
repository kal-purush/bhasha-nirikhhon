/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.ulatina.controllers;

import edu.ulatina.objects.CustomersTO;
import edu.ulatina.services.ServiceCustomerTO;
import java.util.*;
import javax.faces.application.FacesMessage;
import javax.faces.bean.ManagedBean;
import javax.faces.bean.ViewScoped;
import javax.faces.component.UIComponent;
import javax.faces.component.UIInput;
import javax.faces.context.FacesContext;
import javax.faces.event.AjaxBehaviorEvent;

/**
 *
 * @author Vivi
 */
@ManagedBean(name = "customerController")
@ViewScoped
public class CustomerController {
    
    private ServiceCustomerTO serviceCustomerTO;
    private CustomersTO selectedCustomerTO;
    private boolean viewDisabledClient;

    public CustomersTO getSelectedCustomerTO() {
        return selectedCustomerTO;
    }

    public void setSelectedCustomerTO(CustomersTO selectedCustomerTO) {
        this.selectedCustomerTO = selectedCustomerTO;
    }

    public boolean isViewDisabledClient() {
        return viewDisabledClient;
    }

    public void setViewDisabledClient(boolean viewDisabledClient) {
        this.viewDisabledClient = viewDisabledClient;
    }
    
     public void viewDisabledMessage(AjaxBehaviorEvent event) {
        UIComponent component = event.getComponent();
        if (component instanceof UIInput) {
            UIInput inputComponent = (UIInput) component;
            Boolean value = (Boolean) inputComponent.getValue();
            String summary = value ? "Visualizando los deshabilitados" : "Visualizando los habilitados";
            FacesContext.getCurrentInstance().addMessage(null, new FacesMessage(summary));
        }
    }
    
    public List<CustomersTO> getCustomerList() {
        List<CustomersTO> returnList = new ArrayList<>();
        try {
            returnList = serviceCustomerTO.select(1);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return returnList;
    }

    public List<CustomersTO> getDisableCustomerList() {
        List<CustomersTO> returnList = new ArrayList<>();
        try {
            returnList = serviceCustomerTO.select(0);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return returnList;
    }
    
}