package service;

import com.model;



import javax.ws.rs.*; 
import javax.ws.rs.core.MediaType;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.parser.Parser;

//For JSON
import com.google.gson.*;

@Path("/Pay")
public class service {
	
	model paymentObj = new model(); 
	@POST
	@Path("/insert") 
	@Consumes(MediaType.APPLICATION_FORM_URLENCODED) 
	@Produces(MediaType.TEXT_PLAIN) 
	public String insertPayment(@FormParam("CardType") String CardType, 
	 @FormParam("CardNum") String CardNum, 
	 @FormParam("ExpiryDate") String ExpiryDate,
	 @FormParam("CVC") String CVC) 
	{ 
	 String output = paymentObj.insertPayment(CardType, CardNum, ExpiryDate, CVC); 
	return output; 
	}
	
	@GET
	@Path("/read") 
	@Produces(MediaType.TEXT_HTML) 
	public String readPayment() 
	 { 
	 return paymentObj.readPayment(); 
	}
	
	

	@PUT
	@Path("/update") 
	@Consumes(MediaType.APPLICATION_JSON) 
	@Produces(MediaType.TEXT_PLAIN) 
	public String updatePayment(String paymentData) { 
	//Convert the input string to a JSON object 
	 JsonObject paymentObject = new JsonParser().parse(paymentData).getAsJsonObject(); 
	//Read the values from the JSON object
	 String PaymentId = paymentObject.get("PaymentId").getAsString(); 
	 String CardType = paymentObject.get("CardType").getAsString(); 
	 String CardNum = paymentObject.get("CardNum").getAsString(); 
	 String ExpiryDate = paymentObject.get("ExpiryDate").getAsString(); 
	 String CVC = paymentObject.get("CVC").getAsString(); 
	 String output = paymentObj.updatePayment(PaymentId, CardType, CardNum, ExpiryDate, CVC); 
	return output; 
	}
	
	@DELETE
	@Path("/delete") 
	@Consumes(MediaType.APPLICATION_XML) 
	@Produces(MediaType.TEXT_PLAIN) 
	public String deletePayment(String paymentData) 
	{ 
	//Convert the input string to an XML document
	 Document doc = Jsoup.parse(paymentData, "", Parser.xmlParser()); 
	 
	//Read the value from the element <itemID>
	 String PaymentId = doc.select("PaymentId").text(); 
	 String output = paymentObj.deletePayment(PaymentId); 
	return output; 
	}

}