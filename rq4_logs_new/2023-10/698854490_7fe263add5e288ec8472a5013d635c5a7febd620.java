
package edu.latina.controllers;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import javax.faces.bean.ManagedBean;
import javax.faces.bean.SessionScoped;
import javax.faces.bean.ViewScoped;
import javax.faces.context.FacesContext;
import javax.servlet.http.HttpServletRequest;

@ManagedBean(name = "logInController")
@ViewScoped
public class LogInController implements Serializable{
    private String user;
    private String pasword;

    public LogInController() {
    }

    public LogInController(String user, String pasword) {
        this.user = user;
        this.pasword = pasword;
    }

    public String getUser() {
        return user;
    }

    public void setUser(String user) {
        this.user = user;
    }

    public String getPasword() {
        return pasword;
    }

    public void setPasword(String pasword) {
        this.pasword = pasword;
    }
    
     public void redirect(String rute) {
        HttpServletRequest request;
        try {
            request = (HttpServletRequest) FacesContext.getCurrentInstance().getExternalContext().getRequest();
            FacesContext.getCurrentInstance().getExternalContext().redirect(request.getContextPath() + rute);
        } catch (Exception e) {

        }
    }
     
     public void logIn() {
         this.redirect("/faces/landingPage.xhtml");
     }
     
     public void logOut() {
        try {
            FacesContext.getCurrentInstance().getExternalContext().invalidateSession();

            HttpServletRequest request = (HttpServletRequest) FacesContext.getCurrentInstance().getExternalContext().getRequest();
            FacesContext.getCurrentInstance().getExternalContext().redirect(request.getContextPath() + "/faces/index.xhtml?faces-redirect=true");
        } catch (Exception e) {
            e.printStackTrace();
        }
    }
     
     public List<demo> crearDemo(){
         List<demo> listaDemo = new ArrayList<>();
         
         for(int i = 0; i < 6; i++){
             demo newDemo = new demo("test " + i, "test " + i, "test " + i, "test " + i, "test " + i, "test " + i, "test " + i, "test " + i, i, i);
             listaDemo.add(newDemo);
         }
         return listaDemo;
     }
}