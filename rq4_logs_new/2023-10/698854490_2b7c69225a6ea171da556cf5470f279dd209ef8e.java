package edu.ulatina.controllers;

import com.sun.javafx.scene.control.skin.VirtualFlow;
import edu.ulatina.objects.SiteTO;
import edu.ulatina.services.ServiceSite;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.faces.application.FacesMessage;
import javax.faces.bean.ManagedBean;
import javax.faces.bean.ViewScoped;
import javax.faces.component.UIComponent;
import javax.faces.component.UIInput;
import javax.faces.context.FacesContext;
import javax.faces.event.AjaxBehaviorEvent;
import org.primefaces.PrimeFaces;

@ManagedBean(name = "siteController")
@ViewScoped
public class SiteController implements Serializable {

    private boolean viewDisabledSite = false;
    private ServiceSite serv;
    private SiteTO selectedSite = new SiteTO();

    public SiteController() {
    }

    public ServiceSite getServ() {
        return serv;
    }

    public void setServ(ServiceSite serv) {
        this.serv = serv;
    }

    public SiteTO getSelectedSite() {
        return selectedSite;
    }

    public void setSelectedSite(SiteTO selectedSite) {
        this.selectedSite = selectedSite;
    }

    //isViewDisabledSite setViewDisabledSite y viewDisabledMessage es para los toggleSwitch
    public boolean isViewDisabledSite() {
        return viewDisabledSite;
    }

    public void setViewDisabledSite(boolean viewDisabledSite) {
        this.viewDisabledSite = viewDisabledSite;
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

    public List<SiteTO> getSiteList() {
        List<SiteTO> returnList = new ArrayList<>();
        try {
            returnList = serv.select(1);
        } catch (Exception ex) {
            Logger.getLogger(SiteController.class.getName()).log(Level.SEVERE, null, ex);
        }
        SiteTO prueba = new SiteTO(1000, "prueba", "province", "canton", "adress", 0, "Habilitado");
        returnList.add(prueba);
        return returnList;
    }

    public List<SiteTO> getDisableSiteList() {
        List<SiteTO> returnList = new ArrayList<>();
        try {
            returnList = serv.select(0);
        } catch (Exception ex) {
            Logger.getLogger(SiteController.class.getName()).log(Level.SEVERE, null, ex);
        }
        SiteTO prueba = new SiteTO(1000, "Deshabilitado", "province", "canton", "adress", 0, "Desabilitado");
        returnList.add(prueba);
        return returnList;
    }

    public void createNewSite() {
        boolean flag = true;
        if (selectedSite.getName() == null || selectedSite.getName().equals("")) {
            FacesContext.getCurrentInstance().addMessage("sticky-key", new FacesMessage(FacesMessage.SEVERITY_ERROR, "ERROR", "El campo de nombre esta vacio"));
            flag = false;
        }
        if (selectedSite.getProvince() == null || selectedSite.getProvince().equals("")) {
            FacesContext.getCurrentInstance().addMessage("sticky-key", new FacesMessage(FacesMessage.SEVERITY_ERROR, "ERROR", "El campo de provincia esta vacio"));
            flag = false;
        }
        if (selectedSite.getCanton() == null || selectedSite.getCanton().equals("")) {
            FacesContext.getCurrentInstance().addMessage("sticky-key", new FacesMessage(FacesMessage.SEVERITY_ERROR, "ERROR", "El campo de canton esta vacio"));
            flag = false;
        }
        if (selectedSite.getAdress() == null || selectedSite.getAdress().equals("")) {
            FacesContext.getCurrentInstance().addMessage("sticky-key", new FacesMessage(FacesMessage.SEVERITY_ERROR, "ERROR", "El campo de adress esta vacio"));
            flag = false;
        }

        if (flag) {
            System.out.println("Estoy salvando al usuario");

            try {
                this.serv.insert(selectedSite);
            } catch (Exception ex) {
                Logger.getLogger(SiteController.class.getName()).log(Level.SEVERE, null, ex);
                FacesContext.getCurrentInstance().addMessage("sticky-key", new FacesMessage(FacesMessage.SEVERITY_ERROR, "ERROR", "Error al insertar en base de datos"));
            }
            this.selectedSite = new SiteTO();
            PrimeFaces.current().executeScript("PF('manageSiteContent').hide()");
        }

    }
}