/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package actions.accounts;

import com.opensymphony.xwork2.ActionSupport;
import java.util.logging.Level;
import java.util.logging.Logger;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;
import org.apache.struts2.interceptor.ServletRequestAware;

/**
 *
 * @author sergio
 */
public class SigninAction extends ActionSupport implements ServletRequestAware {
    
    private HttpServletRequest request;
    private String username;
    private String password;

    public String getUsername() {
        return username;
    }

    public void setUsername(String username) {
        this.username = username;
    }

    public String getPassword() {
        return password;
    }

    public void setPassword(String password) {
        this.password = password;
    }
   
    @Override
    public void setServletRequest(HttpServletRequest hsr) {
        this.request = hsr;
    }
    
    @Override
    public String execute() throws Exception {
        return SUCCESS;
    }
    
    public String check() throws Exception {
        try {
            request.login(username, password);
            addActionMessage("Has iniciado sessión con éxito");
            return SUCCESS;
        } catch (ServletException e) {
            Logger.getLogger(SigninAction.class.getName()).log(Level.INFO, e.getMessage());
            addActionError("Debes introducir las credenciales de un usuario disponible en el sistema");
            return INPUT;
        }
    }

}