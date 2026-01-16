package services;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.stream.Collector;
import java.util.stream.Collectors;

import javax.servlet.ServletContext;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.QueryParam;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.sun.org.apache.bcel.internal.generic.SWITCH;

import beans.Apartment;
import beans.Comment;
import beans.User;
import config.PathConfig;
import util.apartment.ApartmentDateComparator;
import util.apartment.ApartmentGuestsComparator;
import util.apartment.ApartmentHostComparator;
import util.apartment.ApartmentRoomsComparator;
import util.apartment.ApartmentTypeComparator;
import util.apartment.Order;

@Path("/apartment")
public class ApartmentService {

	@Context
	ServletContext ctx;
	
	@Path("/test")
	@GET
	@Produces(MediaType.TEXT_PLAIN)
	public String test() {
		return "TESTT";
	}
	
	@Path("")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response getAllApartments() throws JsonParseException, JsonMappingException, IOException {
		ArrayList<Apartment> apartments = readApartments();
		return Response.status(Response.Status.OK).entity(apartments).build();
	}
	
	@Path("/search")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response searchApartments(@QueryParam("value") String value, @QueryParam("sort") String sort) throws JsonParseException, JsonMappingException, IOException {
		ArrayList<Apartment> apartments = readApartments();
		ArrayList<Apartment> searchedApartments = (ArrayList<Apartment>) apartments.stream().filter(apartment -> {
			return apartment.isActive() && (apartment.getLocation().getAddress().getCity().contains(value) || apartment.getLocation().getAddress().getStreet().contains(value));
		}).collect(Collectors.toList());
		switch(sort) {
		case "DATE_ASC":
			searchedApartments.sort(new ApartmentDateComparator(Order.ASC));
			break;
		
		case "DATE_DESC":
			searchedApartments.sort(new ApartmentDateComparator(Order.DESC));
			break;
		
		case "GUEST_ASC":
			searchedApartments.sort(new ApartmentGuestsComparator(Order.ASC));
			break;
			
		case "GUEST_DESC":
			searchedApartments.sort(new ApartmentGuestsComparator(Order.DESC));
			break;
		
		case "HOST_ASC":
			searchedApartments.sort(new ApartmentHostComparator(Order.ASC));
			break;
		
		case "HOST_DESC":
			searchedApartments.sort(new ApartmentHostComparator(Order.DESC));
			break;
		
		case "PRICE_ASC":
			searchedApartments.sort(new ApartmentHostComparator(Order.ASC));
			break;
			
		case "PRICE_DESC":
			searchedApartments.sort(new ApartmentHostComparator(Order.DESC));
			break;
		
		case "ROOM_ASC":
			searchedApartments.sort(new ApartmentRoomsComparator(Order.ASC));
			break;
			
		case "ROOM_DESC":
			searchedApartments.sort(new ApartmentRoomsComparator(Order.DESC));
			break;
		
		case "TYPE_ASC":
			searchedApartments.sort(new ApartmentTypeComparator(Order.ASC));
			break;
			
		case "TYPE_DESC":
			searchedApartments.sort(new ApartmentTypeComparator(Order.DESC));
			break;
		}
		return Response.status(Response.Status.OK).entity(searchedApartments).build();
	}

	@Path("/{id}/comments")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response getComments(@PathParam("id") String id) throws JsonParseException, JsonMappingException, IOException {
		Apartment apartment = getApartmentById(id);
		if (apartment == null) 
			return Response.status(Response.Status.NOT_FOUND).build();
		ArrayList<Comment> comments = getCommentByApartmentId(id);
		return Response.status(Response.Status.OK).entity(comments).build();
	}
 	
	private Apartment getApartmentById(String id) throws JsonParseException, JsonMappingException, IOException {
		ArrayList<Apartment> apartments = readApartments();
		for (Apartment apartment : apartments) {
			if (apartment.getId().equals(id))
				return apartment;
		}
		return null;
	}
	
	private ArrayList<Apartment> readApartments() throws JsonParseException, JsonMappingException, IOException{
		ObjectMapper mapper = new ObjectMapper();
		File apartmentsFile = new File(ctx.getRealPath(".") + PathConfig.APARTMENTS_FILE);
		boolean created = apartmentsFile.createNewFile();
		if (created) 
			mapper.writeValue(apartmentsFile, new ArrayList<Apartment>());
		return mapper.readValue(apartmentsFile, new TypeReference<ArrayList<Apartment>>() {});
	}
	
	private void writeApartments(ArrayList<Apartment> apartments) throws IOException {
		ObjectMapper mapper = new ObjectMapper();
		File apartmentsFile = new File(ctx.getRealPath(".") + PathConfig.APARTMENTS_FILE);
		apartmentsFile.createNewFile();
		mapper.writeValue(apartmentsFile, apartments);
	}
	
	private ArrayList<Comment> readComments() throws JsonParseException, JsonMappingException, IOException{
		ObjectMapper mapper = new ObjectMapper();
		File commentsFile = new File(ctx.getRealPath(".") + PathConfig.COMMENTS_FILE);
		boolean created = commentsFile.createNewFile();
		if (created) 
			mapper.writeValue(commentsFile, new ArrayList<Comment>());
		return mapper.readValue(commentsFile, new TypeReference<ArrayList<Comment>>() {});
	}
	
	private ArrayList<Comment> getCommentByApartmentId(String apartmentId) throws JsonParseException, JsonMappingException, IOException{
		Apartment apartment = getApartmentById(apartmentId);
		ArrayList<Comment> comments = readComments();
		comments.stream().filter(comment -> apartment.getCommentsId().stream().anyMatch(id -> id == comment.getId()));
		return comments;
	}

}