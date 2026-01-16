package com.oasis.poc1.controller;

import java.sql.SQLException;
import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RestController;

import com.oasis.poc1.entity.Oasis_Poc1;
import com.oasis.poc1.service.Poc1DatabaseService;
/**************
 * Class: Poc1DatabaseController 
 * 
 * Purpose: Rest controller class that has functions to communicates with Database.
 * 
 */
@RestController
public class Poc1DatabaseController {
	
	@Autowired
	Poc1DatabaseService service;
	
	/**************
	 * Method: testDatabaseConnection 
	 * Purpose: This method will test the Database Connection status
	 * Input parameters: None
	 * @return Success/Failure string response
	 */
	@GetMapping("/checkDbConnection")
	public String testDatabaseConnection() throws SQLException  {			
		return service.checkDatabaseConnection();
	}
	
	/**************
	 * Method: getAllEntitiesFromDatabase 
	 * Purpose: This method will return all the entities store in DB
	 * Input parameters: None
	 * @return List of Oasis_Poc1
	 */
	@GetMapping("/getAllEntitiesfromDb")
	public List<Oasis_Poc1> getAllEntitiesFromDatabase(){
		List<Oasis_Poc1> entityList = service.getAllEntitiesfromDatabase();
		return entityList;
	}
	
}