package com.example.demo.controller;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RestController;


import com.example.demo.entities.background_sync;
import com.example.demo.services.background_service;


@RestController
public class background_controller {
	
@Autowired
private background_service bs;

@PostMapping("/background_sync/")
public void create(@RequestBody background_sync int1)
{
    
	bs.create(int1);
}
}