package com.qa.intigration;

import javax.inject.Inject;
import javax.ws.rs.DELETE;
import javax.ws.rs.GET;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.PathParam;
import javax.ws.rs.Produces;

import com.qa.service.business.ReceiptService;
@Path("/receipt")
public class ReceiptEndpoint {
	@Inject
	private ReceiptService service;
	
	@Path("/json/{i}")
	@GET
	@Produces({ "application/json" })
	public String getReceipt(@PathParam("i") Long id) {
		return service.getReceipt(id);
	}

	@Path("/json")
	@POST
	@Produces({ "application/json" })
	public String addReceipt(String receipt) {
		return service.createReceipt(receipt);
	}

	@Path("/json/{i}")
	@DELETE
	@Produces({ "application/json" })
	public String deleteAccount(@PathParam("i") Long id) {
		return service.deleteReceipt(id);

	}

	public void setService(ReceiptService service) {
		this.service = service;
	}

}