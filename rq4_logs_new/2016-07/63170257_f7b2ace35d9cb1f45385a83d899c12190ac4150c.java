package controllers;

import play.*;
import play.mvc.*;

import java.util.*;

import models.*;

public class Application extends Controller {

    public static void index() {
        render();
    }
    
    public static void index2() {
    	
    	List<cuaderno> x = cuaderno.findAll();
    	
    	List<temporal> y = temporal.findAll();
    	List<esferografico> z = esferografico.findAll();
    	List<accesorio> a = accesorio.findAll();
    	List<archivador> w = archivador.findAll();
    	
    	    	
        render(x,y,z,a,w);
        
    }


    

    
  
    public static void registrar_usuario( long code,String username , String apellido , String cedula ,String email , String password ){
    	
    Usuario nuevo = new Usuario (code ,username , apellido, cedula , email, password );
    nuevo.save();
    ///flash.put("nuevo","USUARIO REGISTRADO CON EXITO");
    redirect("/Application/index");
   
    
    }
    
    
    
    
	
    public static void registrar_temporal( long code,double precio , String  detalle , int cantidad,double total , double totalfinal){
    	
    total = precio * cantidad;
   /// totalfinal = 0;
    
    
   ///// totalfinal = totalfinal + total;
    
    
    
    
    
    temporal pedido = new temporal(code,precio, detalle, cantidad,total,totalfinal);
    
    	 pedido.save();
    	 redirect("/Application/index2");
    	
    }
    
    
    
    static Usuario connected() {
        if(renderArgs.get("user") != null) {
            return renderArgs.get("user", Usuario.class);
        }
        String username = session.get("user");
        if(username != null) {
            return Usuario.find("byUsername", username).first();
        } 
        return null;
    }
    
    


    public static void login(String  username, String password) {
        Usuario user = Usuario.find("byUsernameAndPassword", username, password).first();
        
      //  if(username.equals("estalin")&&password.equals("madrid")){
        
        	
        //}
        
        if(user != null) {
            session.put("user", user.username);
            flash.success("Bienvenido, " + user.username);
          index2();
        }
        // Oops
        flash.put("username", username);
        flash.error("Login failed");
      index();
      
    }
    
    public static void logout() {
        session.clear();
        index();
    }
    
    
    @Before
    static void addUser() {
       Usuario  user = connected();
        if(user != null) {
            renderArgs.put("user", user);
        }
    }
    
    public static void index1() {
        if(connected() != null) {
           retrinsion_pagina.index();
          
        }
        render();
        
    }
    
    


  
 public static void eliminar_pedido( long id){
    	
    	temporal co = temporal.findById(id);
    	co.delete();    	
    	redirect("/Application/index2");
    	
    }
  
    
    
}
