package au.com.retriever.barking.webservice.client;
/*
 * Copyright:  Retriever Communications Copyright (c) 2017
 * 
 * This document is the property of Retriever Communications Ltd. No 
 * part of it may be circulated, quoted, or reproduced for distribution 
 * outside Retriever Communications Ltd. Any party not previously 
 * authorised, in writing, by Retriever Communications Ltd. to discuss 
 * or use these contents should not disclose these contents. All hard 
 * copies must be returned and soft copies destroyed upon request.
 */

import java.util.UUID;

import javax.xml.ws.BindingProvider;

import au.com.retriever.barking.webservice.client.generated.Job;
import au.com.retriever.barking.webservice.client.generated.ObjectFactory;
import au.com.retriever.barking.webservice.client.generated.Result;
import au.com.retriever.barking.webservice.client.generated.RetrieverBarking;
import au.com.retriever.barking.webservice.client.generated.RetrieverBarkingService;

public class BarkingWebServiceClient
{
	public static void main(String[] args)
	{
		// create the client
		RetrieverBarkingService service = new RetrieverBarkingService();
		RetrieverBarking port = service.getRetrieverBarkingPort();
		
		// set the endpoint and credentials
		BindingProvider portBP = (BindingProvider) port;
		//TODO set to the correct endpoint
		String endpoint = "http://localhost/barking/integration/webservice?extsys=BarkingWSImport";
		portBP.getRequestContext().put(BindingProvider.ENDPOINT_ADDRESS_PROPERTY, endpoint);
		//TODO set the correct credentials
		portBP.getRequestContext().put(BindingProvider.USERNAME_PROPERTY, "username1"); 
		portBP.getRequestContext().put(BindingProvider.PASSWORD_PROPERTY, "password1");
		
		// prepare the request
		ObjectFactory objFactory = new ObjectFactory();
		Job job = objFactory.createJob();
		job.setJobId("Job" + UUID.randomUUID());
		job.setCustomerName("Customer1");
		job.setAddress1("Address1");
		job.setJobDesc("JobDesc1");
		
		// call the web service operation
		Result result = port.createJob(job);
		
		// check the response
		System.out.println("Result success: " + result.isSuccess() + " txId: " + result.getTxId() +
			" Error Msg: " + result.getErrorMsg() + " (Code: " + result.getErrorCode() + ").");
	}
}