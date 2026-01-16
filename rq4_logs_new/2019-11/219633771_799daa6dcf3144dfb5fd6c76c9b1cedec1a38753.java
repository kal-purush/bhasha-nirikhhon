package com.project.controller;
import java.util.List;

import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.ResponseBody;

import com.project.pojo.Users;
@Controller
@RequestMapping(value="/users")
@CrossOrigin(origins="*")
public class LoginController {
	final static Logger logger = Logger.getLogger(LoginController.class);
    private LoginService login;
    @GetMapping(value="getAll.app")
    public ResponseEntity<List<Users>> getAll(){
        return new ResponseEntity<List<Users>>(login.getAll(), HttpStatus.OK);
    }
    
    
    
    
    
    
    
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////
    /////////////////////////////////verify login/////////////////////////////////////////////////////
    @PostMapping(value="/login.app")
    public @ResponseBody Users getUserLogin(@RequestBody Users u) {
        System.out.println(u);
        Users newu=null;
       newu=login.getUsernameValue(u.getUsername());
       if(u.getPassword().equals(newu.getPassword())) {
    	   logger.info(u.getUsername() + " user has logged in");
    	   return u;
       }
        return null;
    }
    
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////
    ////////////////////////// inserting information //////////////////////////////////////////////////////
    @PostMapping(value="/post.app")
    public @ResponseBody Users InsertUser(@RequestBody Users u) {
        System.out.println(u);
        login.InsertUser(u);
        return u;
    }
    
    
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////
    ///////////////////////////////////////////////////////////////////////////////////////////////////////////
    
    
    public LoginService getLogin() {
        return login;
    }
    
    
    
    public void setLogin(LoginService login) {
        this.login = login;
    }
    @Autowired
    public LoginController() {
        System.out.println("controller");
    }
    
}