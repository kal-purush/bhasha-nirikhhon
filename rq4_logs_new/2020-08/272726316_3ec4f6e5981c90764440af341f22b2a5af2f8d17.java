package com.gslab.talent.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.CrossOrigin;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.gslab.talent.model.Panel;
import com.gslab.talent.service.PanelService;

@RestController
@CrossOrigin(allowedHeaders = "*")
@RequestMapping(value = "/v1")
public class PanelController {
	
	@Autowired
	PanelService panelServiceObj;
	
	@PostMapping(value = "/panel")
	public ResponseEntity<Void> createNewPanel(@RequestBody Panel panel)
	{
		panelServiceObj.createNewPanel(panel);
		return new ResponseEntity<Void>(HttpStatus.CREATED);
	}
}