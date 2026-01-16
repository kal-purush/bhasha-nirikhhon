package br.com.ardaexperience.service;

import br.com.ardaexperience.bean.ClienteRemote;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import javax.ejb.EJB;
import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;

@Path("login")
public class LoginResource {

    @EJB
    private ClienteRemote clienteRemote;

    Gson gson = new GsonBuilder().setPrettyPrinting().create();

    
    @POST
    @Produces(MediaType.APPLICATION_JSON)
    @Consumes(MediaType.APPLICATION_JSON)
    public String validaUsuario(@PathParam("usuario") String usuario, @PathParam("senha") String senha){
        return gson.toJson(clienteRemote.validar(usuario, senha));
    }
}