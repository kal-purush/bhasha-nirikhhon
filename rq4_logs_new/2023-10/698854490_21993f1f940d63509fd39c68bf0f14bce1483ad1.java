
package edu.ulatina.controllers;


import edu.ulatina.objects.SiteTO;
import edu.ulatina.objects.UnitTO;
import edu.ulatina.services.ServiceUnitTO;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import javax.faces.application.FacesMessage;
import javax.faces.bean.ManagedBean;
import javax.faces.component.UIComponent;
import javax.faces.component.UIInput;
import javax.faces.context.FacesContext;
import javax.faces.event.AjaxBehaviorEvent;
import javax.faces.view.ViewScoped;

/**
 *
 * @author Dylan
 */

@ManagedBean (name = "unitController")
@ViewScoped

public class UnitController implements Serializable {
    
    private ServiceUnitTO serv = new ServiceUnitTO();
    private boolean viewDisableUnit = false;
    private UnitTO selectedUnit = new UnitTO();

    public ServiceUnitTO getServ() {
        return serv;
    }

    public void setServ(ServiceUnitTO serv) {
        this.serv = serv;
    }

    public boolean isViewDisableUnit() {
        return viewDisableUnit;
    }

    public void setViewDisableUnit(boolean viewDisableUnit) {
        this.viewDisableUnit = viewDisableUnit;
    }

    public UnitTO getSelectedUnit() {
        return selectedUnit;
    }

    public void setSelectedUnit(UnitTO selectedUnit) {
        this.selectedUnit = selectedUnit;
    }
    
    public List<UnitTO> getUnitList() {
        List<UnitTO> returnList = new ArrayList<>();
        try {
            returnList = serv.select(1);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return returnList;
    }
    
    public List<UnitTO> getDisableUnitList() {
        List<UnitTO> returnList = new ArrayList<>();
        try {
            returnList = serv.select(0);
        } catch (Exception ex) {
            ex.printStackTrace();
        }
        return returnList;
    }
    
    public void viewDisabledMessageUnit(AjaxBehaviorEvent event) {
        UIComponent component = event.getComponent();
        if (component instanceof UIInput) {
            UIInput inputComponent = (UIInput) component;
            Boolean value = (Boolean) inputComponent.getValue();
            String summary = value ? "Visualizando los deshabilitados" : "Visualizando los habilitados";
            FacesContext.getCurrentInstance().addMessage(null, new FacesMessage(summary));
        }
    }
    
    
    
}