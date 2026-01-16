package com.oasis.poc1.controller;

import java.util.List;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.oasis.poc1.entity.Oasis_Poc2;
import com.oasis.poc1.service.Poc2DatabaseService;

/**************
 * Class: Poc2DatabaseController 
 * 
 * Purpose: Rest controller class that has functions to communicates with Oasis_Poc2 Table.
 *          Oasis_Poc2 : Dynamic UI Template Control
 * 
 */

@CrossOrigin(origins = "https://oasis-poc2-dynamicui.azurewebsites.net")
@RestController
@RequestMapping("/oasis/poc1")
public class Poc2DatabaseController {

	@Autowired
	Poc2DatabaseService service;
	
	/**************
	 * Method: getAllEntitiesFromTable 
	 * Purpose: This method will return all the entities store in Oasis_Poc2 Table
	 * Input parameters: None
	 * @return List<Oasis_Poc2>
	 */
	@GetMapping("/getAllPoc2Entities")
	public List<Oasis_Poc2> getAllEntitiesFromTable(){
		List<Oasis_Poc2> entityList = service.getAllEntitiesFromTable();
		return entityList;
	}
	
	@PostMapping()
	public void updatePoc2Entities(){
		
	}
}