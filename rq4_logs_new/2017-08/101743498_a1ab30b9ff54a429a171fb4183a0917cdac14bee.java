package de.daug.semanticchess.Controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.MediaType;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RestController;

import de.daug.semanticchess.Service.SparqlService;

@RestController
@RequestMapping("/sparql")
public class SparqlController{
	
	@Autowired
	private SparqlService queryService;
	
//	@RequestMapping(method = RequestMethod.GET)
//	public String getResults(){	
//		return queryService.getResults();
//		
//	}
	
	@RequestMapping(method = RequestMethod.POST, consumes = MediaType.APPLICATION_JSON_VALUE)
	public String getCustomResult(@RequestBody String strQuery){
		System.out.println("Controller: " + strQuery);
		return queryService.getCustomResult(strQuery);
	}
	
}