package com.gslab.talent.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.DeleteMapping;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.PutMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.gslab.talent.constant.Constant;
import com.gslab.talent.model.Position;
import com.gslab.talent.service.PositionService;

@RestController
@CrossOrigin(allowedHeaders = "*")
@RequestMapping(value = "/v1")
public class PositionController {
	
	@Autowired
	PositionService posServiceObj;
	
	@GetMapping(Constant.GET_ALL_POSITION)
	ResponseEntity<?> getAllPosition(){
		return new ResponseEntity<>(posServiceObj.getAllPositions(), HttpStatus.OK);
	}
	
	@GetMapping(Constant.GET_POSITION_BY_ID)
	ResponseEntity<?> getPositionById( @PathVariable int posId){
		return new ResponseEntity<>(posServiceObj.getPositionById(posId), HttpStatus.OK);
	}
	
	@PostMapping(Constant.CREATE_POSITION)
	ResponseEntity<?> createPosition(@RequestBody Position position){
		posServiceObj.createNewPosition(position);
		return new ResponseEntity<>(HttpStatus.CREATED);
	}
	
	@PutMapping(Constant.UPDATE_POSITION)
	ResponseEntity<?> updatePosition(@PathVariable int posId, @RequestBody Position position){
		posServiceObj.updatePosition(position);
		return new ResponseEntity<Void>(HttpStatus.OK);
	}
	
	@DeleteMapping(Constant.DELETE_ALL_POSITION)
	ResponseEntity<?> deleteAllPosition(){
		posServiceObj.deleteAllPosition();
		return new ResponseEntity<Void>(HttpStatus.OK);
	}
	
	@DeleteMapping(Constant.DLEETE_POSITION_BY_ID)
	ResponseEntity<?> deletePostionById(@PathVariable int posId){
		posServiceObj.deletePositionById(posId);
		return new ResponseEntity<Void>(HttpStatus.OK);
	}

}