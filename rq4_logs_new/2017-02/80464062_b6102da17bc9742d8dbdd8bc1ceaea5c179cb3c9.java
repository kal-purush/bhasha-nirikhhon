package rest;
import javax.*;
import javax.ws.rs.*;
import javax.ws.rs.core.MediaType;


import java.util.*; 
import model.*;

import javax.ws.rs.*;

import javax.ejb.Stateless;
import javax.persistence.EntityManager;
import javax.persistence.NoResultException;
import javax.persistence.Parameter;
import javax.persistence.PersistenceContext;
import javax.ws.rs.*;
import javax.ws.rs.core.GenericEntity;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.util.ArrayList;
import java.util.List;




import model.serices.GuestServices;

import static javax.ws.rs.core.Response.Status.*;

@Path("guest") 
@Stateless 

public class GuestResources {
	
	private GuestServices db = new GuestServices();
	@PersistenceContext 
	EntityManager em;
	
	@GET
	@Path("/show")
	@Produces(MediaType.APPLICATION_JSON)
	public List<Guest> getAll(){
		return db.getAll(); //em.createNamedQuery("guest.all", Guest.class).getResultList();
	} 
	
	@POST 
	@Path("/add")
	@Consumes(MediaType.APPLICATION_JSON)
	public Response AddGuest(Guest gosc){
		db.addGuest(gosc);
		em.persist(gosc);
		return Response.ok(gosc.getId()).build();
		
	}
	@GET 
	@Path("/{id}")
	@Produces(MediaType.APPLICATION_JSON)
	public Response getGuest(@PathParam("id") int id){
		try{
			Guest result = (Guest) em.createNamedQuery("guest.id", Guest.class)
				.setParameter("goscId", id)
				.getSingleResult();
			return Response.ok(result).build();
		}catch (NoResultException e) {
			return Response.status(NOT_FOUND).build();
		}
	}
	
	@DELETE 
	@Path("/{id}")
	public Response delete(@PathParam("id") int id){
		try{
			Guest result = (Guest) em.createNamedQuery("guest.id", Guest.class)
				.setParameter("goscId", id)
				.getSingleResult();
			em.remove(result);
			return Response.ok(result).build();
		}catch (NoResultException e) {
			return Response.status(NOT_FOUND).build();
		}
	}
	
	}
