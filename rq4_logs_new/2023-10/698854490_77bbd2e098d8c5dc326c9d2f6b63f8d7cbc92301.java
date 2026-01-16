/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package edu.ulatina.controllers;

/**
 *
 * @author Nwitlyck
 */
import edu.ulatina.security.AESEncryptionDecryption;
import edu.ulatina.objects.UserTO;
import edu.ulatina.services.ServiceUserTO;
import edu.ulatina.services.ServiceDetails;
import javax.faces.bean.*;
import java.util.*;
import java.util.regex.Pattern;
import javax.annotation.PostConstruct;
import javax.faces.application.FacesMessage;
import javax.faces.component.*;
import javax.faces.context.FacesContext;
import javax.faces.event.AjaxBehaviorEvent;
import javax.servlet.http.*;
import org.primefaces.PrimeFaces;

@ManagedBean(name = "userController")
@ViewScoped
public class UserController {

    //objects
    private UserTO selectedUser;
    private boolean viewDisabledUser = false;
    private ServiceUserTO serUserTO;
    private ServiceDetails serDetail;
    private Map<String, Integer> mapRoles;

    //getters/setrs
    public UserTO getSelectedUser() {
        return selectedUser;
    }

    public void setSelectedUser(UserTO selectedUser) {
        this.selectedUser = selectedUser;
    }

    public boolean isViewDisabledUser() {
        return viewDisabledUser;
    }

    public void setViewDisabledUser(boolean viewDisabledUser) {
        this.viewDisabledUser = viewDisabledUser;
    }

    public ServiceUserTO getSerUserTO() {
        return serUserTO;
    }

    public void setSerUserTO(ServiceUserTO serUserTO) {
        this.serUserTO = serUserTO;
    }

    public ServiceDetails getSerDetail() {
        return serDetail;
    }

    public void setSerDetail(ServiceDetails serDetail) {
        this.serDetail = serDetail;
    }

    public Map<String, Integer> getMapRoles() {
        return mapRoles;
    }

    public void setMapRoles(Map<String, Integer> mapRoles) {
        this.mapRoles = mapRoles;
    }

    //Special Gets/sets
    public List<UserTO> getUserList() {
        List<UserTO> returnList;
        try {
            returnList = serUserTO.select(1);
        } catch (Exception ex) {
            returnList = new ArrayList<>();
            ex.printStackTrace();
        }
        return returnList;
    }

    public List<UserTO> getDisableUserList() {
        List<UserTO> returnList;
        try {
            returnList = serUserTO.select(0);
        } catch (Exception ex) {
            returnList = new ArrayList<>();
            ex.printStackTrace();
        }
        return returnList;
    }

    public String getRoleNameByRoleId(int roleId) {
        String roleName = "not found";
        for (Map.Entry<String, Integer> entry : mapRoles.entrySet()) {
            if (entry.getValue() == roleId) {
                roleName = entry.getKey();
            }
        }
        return roleName;
    }

    //metods
    @PostConstruct
    public void initialize() {
        serUserTO = new ServiceUserTO();
        serDetail = new ServiceDetails();
        selectedUser = new UserTO();
        fillRoleMap();
    }

    private void fillRoleMap() {
        try {
            mapRoles = new ServiceDetails().select(1);
        } catch (Exception e) {
            mapRoles = new HashMap<>();
            e.printStackTrace();
        }
    }

    public void insertUser() {

        if (!notNull()) {
            return;
        }

        if (!followMetrics()) {
            return;
        }

        selectedUser.setPassword(new AESEncryptionDecryption().encrypt(selectedUser.getPassword()));

        try {
            this.serUserTO.insert(selectedUser);
        } catch (Exception ex) {
            ex.printStackTrace();
            FacesContext.getCurrentInstance().addMessage("sticky-key", new FacesMessage(FacesMessage.SEVERITY_ERROR, "ERROR", "Error al hacer insert en base de datos"));
        } finally {
            selectedUser = new UserTO();
        }
    }

    public void updateUser() {

        if (!notNull()) {
            return;
        }

        if (!followMetrics()) {
            return;
        }

        selectedUser.setPassword(new AESEncryptionDecryption().encrypt(selectedUser.getPassword()));

        try {
            this.serUserTO.update(selectedUser);
        } catch (Exception ex) {
            ex.printStackTrace();
            FacesContext.getCurrentInstance().addMessage("sticky-key", new FacesMessage(FacesMessage.SEVERITY_ERROR, "ERROR", "Error al hacer update en base de datos"));
        } finally {
            selectedUser = new UserTO();
        }
    }

    public void disableUser() {
        try {
            serUserTO.delete(selectedUser);
        } catch (Exception ex) {
            ex.printStackTrace();
            FacesContext.getCurrentInstance().addMessage("sticky-key", new FacesMessage(FacesMessage.SEVERITY_ERROR, "ERROR", "Error al desabilitar el usuario en base de datos"));
        }
        this.selectedUser = new UserTO();

    }

    public void enableUser() {
        try {
            serUserTO.enable(selectedUser);
        } catch (Exception ex) {
            ex.printStackTrace();
            FacesContext.getCurrentInstance().addMessage("sticky-key", new FacesMessage(FacesMessage.SEVERITY_ERROR, "ERROR", "Error al desabilitar el usuario en base de datos"));
        }
        this.selectedUser = new UserTO();
    }

    private boolean notNull() {

        if (selectedUser.getName().isEmpty() || selectedUser.getName() == null) {
            System.out.println("edu.ulatina.controllers.UserController.notNull() " + selectedUser.getName());
            FacesContext.getCurrentInstance().addMessage("sticky-key", new FacesMessage(FacesMessage.SEVERITY_WARN, "Valor Nulo", "El nombre esta vacio"));
            return false;
        }
        if (selectedUser.getLastname().isEmpty() || selectedUser.getLastname() == null) {
            FacesContext.getCurrentInstance().addMessage("sticky-key", new FacesMessage(FacesMessage.SEVERITY_WARN, "Valor Nulo", "El apellido esta vacio"));
            return false;
        }
        if (selectedUser.getEmail().isEmpty() || selectedUser.getEmail() == null) {
            FacesContext.getCurrentInstance().addMessage("sticky-key", new FacesMessage(FacesMessage.SEVERITY_WARN, "Valor Nulo", "El correo esta vacio"));
            return false;
        }
        if (selectedUser.getRole() == 0) {
            FacesContext.getCurrentInstance().addMessage("sticky-key", new FacesMessage(FacesMessage.SEVERITY_WARN, "Valor Nulo", "Seleccione un rol"));
            return false;
        }

        return true;
    }

    private boolean followMetrics() {

        String regexPatternEmail = "^(?=.{1,64}@)[A-Za-z0-9_-]+(\\.[A-Za-z0-9_-]+)*@" + "[^-][A-Za-z0-9-]+(\\.[A-Za-z0-9-]+)*(\\.[A-Za-z]{2,})$";

        if (!Pattern.compile(regexPatternEmail).matcher(this.selectedUser.getEmail()).matches()) {

            FacesContext.getCurrentInstance().addMessage(null, new FacesMessage(FacesMessage.SEVERITY_WARN, "Invalido", "El correo ingresado no es valido"));
            return false;
        }

        for (UserTO user : getUserList()) {
            if (selectedUser.getEmail() == user.getEmail()) {
                FacesContext.getCurrentInstance().addMessage(null, new FacesMessage(FacesMessage.SEVERITY_WARN, "Invalido", "El correo ya existe"));
                return false;
            }
        }
        
        for (UserTO user : getDisableUserList()) {
            if (selectedUser.getEmail() == user.getEmail()) {
                FacesContext.getCurrentInstance().addMessage(null, new FacesMessage(FacesMessage.SEVERITY_WARN, "Invalido", "El correo ya existe"));
                return false;
            }
        }

        if (selectedUser.getPassword().length() >= 16 && selectedUser.getPassword().length() <= 8) {

            FacesContext.getCurrentInstance().addMessage(null, new FacesMessage(FacesMessage.SEVERITY_WARN, "Invalido", "La contraseña ocupa ser de entre 8 y 16 caracteres"));
            return false;
        }
        return true;
    }

    public void resetSelectUser() {
        this.selectedUser = new UserTO();
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