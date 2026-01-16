package com.gslab.talent.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.gslab.talent.constant.Constant;
import com.gslab.talent.model.Project;
import com.gslab.talent.service.ProjectService;

@RestController
@CrossOrigin(allowedHeaders = "*")
@RequestMapping(value = "/v1")
public class ProjectController {
	
	@Autowired
	ProjectService projService;
	
	@GetMapping(Constant.GET_ALL_PROJECT)
	ResponseEntity<?> getAllProject(){
		return new ResponseEntity<>(projService.getAllProject(), HttpStatus.OK);
	}
	
	@GetMapping(Constant.GET_PROJECT_BY_ID)
	ResponseEntity<?> getProjectById(@PathVariable int projId){
		return new ResponseEntity<>(projService.getProjectById(projId), HttpStatus.OK);
	}
	
	@PostMapping(Constant.CREATE_PROJECT)
	ResponseEntity<?> createProject(@RequestBody Project project){
		projService.createProject(project);
		return new ResponseEntity<>(HttpStatus.CREATED);
	}
	
	@DeleteMapping(Constant.DLEETE_PROJECT_BY_ID )
	ResponseEntity<?> deleteProjectById(@PathVariable int projId ){
		projService.deleteProject(projId);
		return new ResponseEntity<>(HttpStatus.OK);
	}
	
	@DeleteMapping(Constant.DELETE_ALL_PROJECT)
	ResponseEntity<?> deleteAllProject( ){
		projService.deleteAll();
		return new ResponseEntity<>(HttpStatus.OK);
	}
	
	
	

}