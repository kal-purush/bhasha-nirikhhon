package com.carwash.service;

import java.util.List;

import javax.jws.WebService;
import javax.ws.rs.GET;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

import com.carwash.model.LabelValue;


@WebService
public interface MetaDataService {

	@GET
	@Path("/metadata/getArea/{query}")
	@Produces("application/json")
	List<LabelValue> getAreaSuggestion(@PathParam("query") String query);
}