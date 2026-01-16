package services;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;

import javax.servlet.ServletContext;
import javax.servlet.http.HttpServletRequest;
import javax.ws.rs.Consumes;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.PUT;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;
import javax.ws.rs.core.Context;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;

import com.fasterxml.jackson.core.JsonGenerationException;
import com.fasterxml.jackson.core.JsonParseException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.JsonMappingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import beans.Amenity;
import beans.Apartment;
import beans.Role;
import beans.User;
import config.PathConfig;

@Path("/amenity")
public class AmenityService {

	@Context
	ServletContext ctx;

	@Context
	HttpServletRequest request;
	
	@Path("")
	@POST
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response addAmenity(Amenity amenity) throws JsonGenerationException, JsonMappingException, IOException {
		User loggedUser = (User) request.getSession().getAttribute("loggedUser");
		if (loggedUser == null)
			return Response.status(Response.Status.UNAUTHORIZED).build();
		if (loggedUser.getRole() != Role.ADMIN)
			return Response.status(Response.Status.FORBIDDEN).build();
		
		Amenity a = new Amenity(amenity.getName());
		ArrayList<Amenity> amenities = readAmenities();
		amenities.add(a);
		writeAmenities(amenities);
		return Response.status(Response.Status.OK).entity(a).build();
	}
	
	@Path("")
	@GET
	@Produces(MediaType.APPLICATION_JSON)
	public Response getAllAmenities() throws JsonGenerationException, JsonMappingException, IOException {
		ArrayList<Amenity> amenities = readAmenities();
		return Response.status(Response.Status.OK).entity(amenities).build();
	}
	
	@Path("/{id}")
	@PUT
	@Consumes(MediaType.APPLICATION_JSON)
	@Produces(MediaType.APPLICATION_JSON)
	public Response editAmenity(@PathParam("id") String id, Amenity amenity) throws IOException {
		User loggedUser = (User) request.getSession().getAttribute("loggedUser");
		if (loggedUser == null)
			return Response.status(Response.Status.UNAUTHORIZED).build();
		if (loggedUser.getRole() != Role.ADMIN)
			return Response.status(Response.Status.FORBIDDEN).build();
		ArrayList<Amenity> amenities = readAmenities();
		for (Amenity a : amenities) {
			if (a.getId().equals(id)) {
				a.setName(amenity.getName());
				writeAmenities(amenities);
				return Response.status(Response.Status.OK).entity(a).build();
			}
		}
		return Response.status(Response.Status.NOT_FOUND).build();
		
	}
	
	@Path("/{id}")
	@DELETE
	public Response deleteAmenity(@PathParam("id") String id) throws JsonGenerationException, JsonMappingException, IOException {
		User loggedUser = (User) request.getSession().getAttribute("loggedUser");
		if (loggedUser == null)
			return Response.status(Response.Status.UNAUTHORIZED).build();
		if (loggedUser.getRole() != Role.ADMIN)
			return Response.status(Response.Status.FORBIDDEN).build();
		
		Amenity amenity = getAmenityById(id);
		if (amenity == null) 
			return Response.status(Response.Status.NOT_FOUND).build();
		
		ArrayList<Amenity> amenities = readAmenities();
		amenities.stream().filter(a -> !a.getId().equals(id));
		writeAmenities(amenities);
		
		ArrayList<Apartment> apartments = readApartments();
		apartments.forEach(apartment -> apartment.getAmenitiesId().stream().filter(amenityId -> !amenityId.equals(id)));
		writeApartments(apartments);
		
		return Response.status(Response.Status.OK).entity(amenity).build();
	}
	
	
	private ArrayList<Amenity> readAmenities() throws JsonGenerationException, JsonMappingException, IOException{
		ObjectMapper mapper = new ObjectMapper();
		File amenitiesFile = new File(ctx.getRealPath(".") + PathConfig.AMENITIES_FILE);
		boolean created = amenitiesFile.createNewFile();
		if (created)
			mapper.writeValue(amenitiesFile, new ArrayList<Amenity>());
		return mapper.readValue(amenitiesFile, new TypeReference<ArrayList<Amenity>>() {
		});
	}
	
	private void writeAmenities(ArrayList<Amenity> amenities) throws IOException {
		ObjectMapper mapper = new ObjectMapper();
		File amenitiesFile = new File(ctx.getRealPath(".") + PathConfig.AMENITIES_FILE);
		amenitiesFile.createNewFile();
		mapper.writeValue(amenitiesFile, amenities);
	}
	
	private Amenity getAmenityById(String id) throws JsonGenerationException, JsonMappingException, IOException {
		ArrayList<Amenity> amenities = readAmenities();
		for (Amenity amenity : amenities) {
			if (amenity.getId().equals(id))
				return amenity;
		}
		return null;
	}
	
	private ArrayList<Apartment> readApartments() throws JsonParseException, JsonMappingException, IOException {
		ObjectMapper mapper = new ObjectMapper();
		File apartmentsFile = new File(ctx.getRealPath(".") + PathConfig.APARTMENTS_FILE);
		boolean created = apartmentsFile.createNewFile();
		if (created)
			mapper.writeValue(apartmentsFile, new ArrayList<Apartment>());
		return mapper.readValue(apartmentsFile, new TypeReference<ArrayList<Apartment>>() {
		});
	}

	private void writeApartments(ArrayList<Apartment> apartments) throws IOException {
		ObjectMapper mapper = new ObjectMapper();
		File apartmentsFile = new File(ctx.getRealPath(".") + PathConfig.APARTMENTS_FILE);
		apartmentsFile.createNewFile();
		mapper.writeValue(apartmentsFile, apartments);
	}
}