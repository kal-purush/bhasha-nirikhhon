

package edu.ulatina.controllers;

import edu.ulatina.security.AESEncryptionDecryption;
import edu.ulatina.objects.UserTO;
import edu.ulatina.services.ServiceUserTO;
import javax.faces.bean.*;
import java.util.*;
import java.util.regex.Pattern;
import javax.annotation.PostConstruct;
import javax.faces.application.FacesMessage;
import javax.faces.component.*;
import javax.faces.context.FacesContext;
import javax.faces.event.AjaxBehaviorEvent;

/**
 *
 * @author Nwitlyck
 */
@ManagedBean(name = "userVetController")
@ViewScoped
public class VeterinaryController {
    
    private UserTO selectedUserVet;
    private boolean viewDisabledVet = false;
    private ServiceUserTO serUserTO;

    public UserTO getSelectedUserVet() {
        return selectedUserVet;
    }

    public void setSelectedUserVet(UserTO selectedUserVet) {
        this.selectedUserVet = selectedUserVet;
    }

    public boolean isViewDisabledVet() {
        return viewDisabledVet;
    }

    public void setViewDisabledVet(boolean viewDisabledVet) {
        this.viewDisabledVet = viewDisabledVet;
    }

    public ServiceUserTO getSerUserTO() {
        return serUserTO;
    }

    public void setSerUserTO(ServiceUserTO serUserTO) {
        this.serUserTO = serUserTO;
    }
    
    //Special Gets/sets
    public List<UserTO> getUserVetList() {
        List<UserTO> returnList;
        try {
            returnList = serUserTO.selectByRole(1,2);
        } catch (Exception ex) {
            returnList = new ArrayList<>();
            ex.printStackTrace();
        }
        return returnList;
    }

    public List<UserTO> getDisableUserVetList() {
        List<UserTO> returnList;
        try {
            returnList = serUserTO.selectByRole(0,2);
        } catch (Exception ex) {
            returnList = new ArrayList<>();
            ex.printStackTrace();
        }
        return returnList;
    }
    
    public String getPassword(){
        return "Password";
    }
    
    public void setPassword(String password){
        if(password != "Password" || !password.isEmpty()){
           selectedUserVet.setPassword(password);
        }
        else{
            String p = new AESEncryptionDecryption().decrypt(selectedUserVet.getPassword());
            selectedUserVet.setPassword(p);
        }
    }
    
    //metods
    @PostConstruct
    public void initialize() {
        serUserTO = new ServiceUserTO();
        selectedUserVet = new UserTO();
    }
    
    

    public void insertUser() {

        if (!notNull()) {
            return;
        }

        if (!followMetrics()) {
            return;
        }

        selectedUserVet.setPassword(new AESEncryptionDecryption().encrypt(selectedUserVet.getPassword()));
        selectedUserVet.setRole(2);

        try {
            this.serUserTO.insert(selectedUserVet);
        } catch (Exception ex) {
            ex.printStackTrace();
            FacesContext.getCurrentInstance().addMessage("sticky-key", new FacesMessage(FacesMessage.SEVERITY_ERROR, "ERROR", "Error al hacer insert en base de datos"));
        } finally {
            selectedUserVet = new UserTO();
        }
    }

    public void updateUser() {
        
        if(selectedUserVet.getPassword().isEmpty()){
            for(UserTO u : getUserVetList()){
                if(u.getId() == selectedUserVet.getId()){
                    selectedUserVet.setPassword(new AESEncryptionDecryption().decrypt(u.getPassword()));
                }
            }
        }
        
        if (!notNull()) {
            return;
        }

        if (!followMetrics()) {
            return;
        }

        selectedUserVet.setPassword(new AESEncryptionDecryption().encrypt(selectedUserVet.getPassword()));

        try {
            this.serUserTO.update(selectedUserVet);
        } catch (Exception ex) {
            ex.printStackTrace();
            FacesContext.getCurrentInstance().addMessage("sticky-key", new FacesMessage(FacesMessage.SEVERITY_ERROR, "ERROR", "Error al hacer update en base de datos"));
        } finally {
            selectedUserVet = new UserTO();
        }
    }
    
    public void disableUserVet() {
        try {
            serUserTO.delete(selectedUserVet);
        } catch (Exception ex) {
            ex.printStackTrace();
            FacesContext.getCurrentInstance().addMessage("sticky-key", new FacesMessage(FacesMessage.SEVERITY_ERROR, "ERROR", "Error al desabilitar el usuario en base de datos"));
        }
        this.selectedUserVet = new UserTO();

    }

    public void enableUserVet() {
        try {
            serUserTO.enable(selectedUserVet);
        } catch (Exception ex) {
            ex.printStackTrace();
            FacesContext.getCurrentInstance().addMessage("sticky-key", new FacesMessage(FacesMessage.SEVERITY_ERROR, "ERROR", "Error al desabilitar el usuario en base de datos"));
        }
        this.selectedUserVet = new UserTO();
    }
    
    

    private boolean notNull() {

        if (selectedUserVet.getName().isEmpty() || selectedUserVet.getName() == null) {
            FacesContext.getCurrentInstance().addMessage("sticky-key", new FacesMessage(FacesMessage.SEVERITY_WARN, "Valor Nulo", "El nombre esta vacio"));
            return false;
        }
        if (selectedUserVet.getEmail().isEmpty() || selectedUserVet.getEmail() == null) {
            FacesContext.getCurrentInstance().addMessage("sticky-key", new FacesMessage(FacesMessage.SEVERITY_WARN, "Valor Nulo", "El correo esta vacio"));
            return false;
        }

        return true;
    }

    private boolean followMetrics() {

        String regexPatternEmail = "^(?=.{1,64}@)[A-Za-z0-9_-]+(\\.[A-Za-z0-9_-]+)*@" + "[^-][A-Za-z0-9-]+(\\.[A-Za-z0-9-]+)*(\\.[A-Za-z]{2,})$";

        if (!Pattern.compile(regexPatternEmail).matcher(this.selectedUserVet.getEmail()).matches()) {

            FacesContext.getCurrentInstance().addMessage(null, new FacesMessage(FacesMessage.SEVERITY_WARN, "Invalido", "El correo ingresado no es valido"));
            return false;
        }

        for (UserTO user : getUserVetList()) {
            if (selectedUserVet.getEmail() == user.getEmail()) {
                FacesContext.getCurrentInstance().addMessage(null, new FacesMessage(FacesMessage.SEVERITY_WARN, "Invalido", "El correo ya existe"));
                return false;
            }
        }
        
        for (UserTO user : getDisableUserVetList()) {
            if (selectedUserVet.getEmail() == user.getEmail()) {
                FacesContext.getCurrentInstance().addMessage(null, new FacesMessage(FacesMessage.SEVERITY_WARN, "Invalido", "El correo ya existe"));
                return false;
            }
        }
        
        if (!(selectedUserVet.getPassword().length() >= 8 && selectedUserVet.getPassword().length() <= 16 )) {
            
            FacesContext.getCurrentInstance().addMessage(null, new FacesMessage(FacesMessage.SEVERITY_WARN, "Invalido", "La contraseña ocupa ser de entre 8 y 16 caracteres"));
            return false;
        }
        return true;
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
}