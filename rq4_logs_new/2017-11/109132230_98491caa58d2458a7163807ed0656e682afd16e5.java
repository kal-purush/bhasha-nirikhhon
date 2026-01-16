package br.com.ardaexperience.service;

import br.com.ardaexperience.bean.CardRemote;
import br.com.ardaexperience.entidade.Card;
import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import javax.ejb.EJB;
import javax.ws.rs.Produces;
import javax.ws.rs.Consumes;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PUT;
import javax.ws.rs.PathParam;
import javax.ws.rs.core.MediaType;

@Path("roteiros/cards")
public class CardsResource {
    
    @EJB
    private CardRemote cardRemote;
    
    Gson gson = new GsonBuilder().setPrettyPrinting().create();
    
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String getJson() {
        return gson.toJson(cardRemote.consultarTodos());
    }
    
    @Path("{id}")
    @GET
    @Produces(MediaType.APPLICATION_JSON)
    public String consultarContatoPorId(@PathParam("id") String idCard) {
        Long id = Long.valueOf(idCard);
        return gson.toJson(cardRemote.consultarPorId(id));
    }

    @PUT
    @Consumes(MediaType.APPLICATION_JSON)
    public String putJson(String cardJson) throws Exception {
        Card card = gson.fromJson(cardJson, Card.class);
        return gson.toJson(cardRemote.atualizar(card));
    }
}