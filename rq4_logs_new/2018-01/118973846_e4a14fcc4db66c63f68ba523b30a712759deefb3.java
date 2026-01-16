/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.syntel.controller;

import java.util.List;
import model.Connector;
import model.OnlineUser;
import org.springframework.stereotype.Controller;
import org.springframework.ui.ModelMap;
import org.springframework.web.bind.annotation.ModelAttribute;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;

/**
 *
 * @author syntel
 */

@Controller("/manageUsers")
//@SessionAttributes("users")

public class ManageUserController {
    
    //@Autowired
   // private ManageUserDAO userMgt;
    
    @RequestMapping("/index.htm")
    public String home(ModelMap model){
        
        return "index";
    }
    

    
    @RequestMapping("/manageUsers.htm")
    public String manageUsers(ModelMap model){
       // userMgt = new ManageUserDAO();
       Connector con = new Connector();
        List <OnlineUser> users = con.getAllUsers();
        model.put("users", users);
        return "manageUsers";
    }
    
    @RequestMapping(value="delete.htm", method=RequestMethod.GET)
    public String deleteUser(ModelMap model, @RequestParam String email){
        Connector con = new Connector();
        con.deleteUserQuery(email);
        List <OnlineUser> users = con.getAllUsers();
        model.put("users", users);
        return "manageUsers";
    }
    
    @RequestMapping(value="enable.htm", method=RequestMethod.GET)
    public String enableUser(ModelMap model, @RequestParam String email){
        Connector con = new Connector();
        con.enableUserQuery(email);
        List <OnlineUser> users = con.getAllUsers();
        model.put("users", users);
        return "manageUsers";
    }
    
     @RequestMapping(value="disable.htm", method=RequestMethod.GET)
    public String disableUser(ModelMap model, @RequestParam String email){
        Connector con = new Connector();
        con.disableUserQuery(email);
        List <OnlineUser> users = con.getAllUsers();
        model.put("users", users);
        return "manageUsers";
    }
    
     @RequestMapping(value="/changepassword.htm", method=RequestMethod.GET)
    public void changePassword(@ModelAttribute("user") OnlineUser user, ModelMap model){
        Connector con = new Connector();
        con.changePasswordQuery(user.getEmail(), user.getPassword());
       // return "changepassword";
    }

    
}