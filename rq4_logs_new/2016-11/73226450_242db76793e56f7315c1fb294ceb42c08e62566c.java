/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package actions.accounts;

import com.google.inject.Inject;
import com.opensymphony.xwork2.ActionSupport;
import com.opensymphony.xwork2.ModelDriven;
import facade.UserFacadeLocal;
import java.util.logging.Level;
import java.util.logging.Logger;
import models.User;

/**
 *
 * @author sergio
 */
public class CreateUserAction extends ActionSupport implements ModelDriven<User>{
    
    @Inject
    private UserFacadeLocal userFacade;
    private User user = new User();

    public String execute() throws Exception {
        Logger.getLogger(SignupAction.class.getName()).log(Level.INFO, "Registrando usuario");
        userFacade.create(user);
        addActionMessage("Usuario registrado con éxito");
        return SUCCESS;
    }
    
    @Override
    public User getModel() {
        return user;
    }
}