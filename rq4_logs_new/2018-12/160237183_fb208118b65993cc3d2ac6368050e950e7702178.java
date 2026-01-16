/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package br.edu.ifrn.web.bean;


import br.edu.ifrn.web.modelo.Usuario;
import javax.enterprise.inject.Model;
import javax.inject.Inject;
import javax.servlet.ServletException;
import javax.servlet.http.HttpServletRequest;

/**
 *
 * @author beatriz
 */
@Model
public class LoginBean {
    
    
    private String email;
    private String senha;
    
    @Inject
    private HttpServletRequest request;
    
    public String efetuarLogin() throws ServletException{
        System.out.println("Login....");
        request.login(email, senha);
        
        return "turma.xhtml";
    }

    public String getEmail() {
        return email;
    }

    public void setEmail(String email) {
        this.email = email;
    }

    public String getSenha() {
        return senha;
    }

    public void setSenha(String senha) {
        this.senha = senha;
    }

    
    
    

}